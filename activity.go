package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// NodeActivity holds activity metrics and delay for a single stream node (collector or distributor).
type NodeActivity struct {
	Base                  string
	Stream                string
	Node                  string
	NodeType              string  // "collector" or "distributor"
	LastActivityTimestamp float64 // Unix epoch seconds
	DelaySec              float64 // (now - timestamp) or 0 if <= minDelay
	LastFile              string
	Success               bool
}

// NodeStoppedStatus holds the heartbeat-based stopped state for a single stream node.
// Stopped is true when the heartbeat file ${node}/control/1/heartbeat is missing,
// mirroring the legacy shell check: [ ! -f "${node}/control/1/heartbeat" ] => STOPPED_INDEX=1.
type NodeStoppedStatus struct {
	Base     string
	Stream   string
	Node     string
	NodeType string // "collector", "distributor", or empty
	Stopped  bool   // true if heartbeat file is missing (node stopped)
}

// NodeFailedStatus holds the failure state for a single stream node.
// Failed is true only when BOTH conditions hold (mirroring legacy FAILED_INDEX logic):
//  1) A trace-log line matching  [NODE] .* (aborted and was disabled|Node index .* failed)
//     is found in TRACEDIR/execution_trace_<STREAM>_<DATE>_*, and
//  2) The node is currently stopped (heartbeat missing).
type NodeFailedStatus struct {
	Base       string
	Stream     string
	Node       string
	NodeType   string  // "collector", "distributor", or empty
	Failed     bool    // true if failure pattern found AND stopped==true
	FailedTime float64 // Unix epoch of the matched failure line (0 if not failed)
}

// StreamNode represents a discovered node inside a stream directory.
type StreamNode struct {
	Base     string // target label or base path
	Stream   string
	Node     string
	NodeDir  string
	NodeType string // "collector", "distributor", or empty
}

// ScanCollectorActivity inspects a collector node directory for storage/1/audit_info,
// extracting the last processed filename and epoch timestamp.
func ScanCollectorActivity(baseLabel, stream, node, nodeDir string, minDelay time.Duration, now time.Time) (NodeActivity, error) {
	auditPath := filepath.Join(nodeDir, "storage", "1", "audit_info")
	data, err := os.ReadFile(auditPath)
	if err != nil {
		return NodeActivity{
			Base:     baseLabel,
			Stream:   stream,
			Node:     node,
			NodeType: "collector",
			LastFile: "-",
			Success:  false,
		}, err
	}

	fields := strings.Fields(string(data))
	if len(fields) < 2 {
		return NodeActivity{
			Base:     baseLabel,
			Stream:   stream,
			Node:     node,
			NodeType: "collector",
			LastFile: "-",
			Success:  false,
		}, fmt.Errorf("malformed audit_info: expected at least 2 fields, got %d", len(fields))
	}

	lastFile := fields[0]
	epoch, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil || epoch <= 0 {
		return NodeActivity{
			Base:     baseLabel,
			Stream:   stream,
			Node:     node,
			NodeType: "collector",
			LastFile: lastFile,
			Success:  false,
		}, fmt.Errorf("invalid epoch timestamp in audit_info: %q", fields[1])
	}

	delay := now.Unix() - epoch
	if delay < 0 {
		delay = 0
	}
	if minDelay > 0 && delay <= int64(minDelay.Seconds()) {
		delay = 0
	}

	return NodeActivity{
		Base:                  baseLabel,
		Stream:                stream,
		Node:                  node,
		NodeType:              "collector",
		LastActivityTimestamp: float64(epoch),
		DelaySec:              float64(delay),
		LastFile:              lastFile,
		Success:               true,
	}, nil
}

// parseDistributorLine checks if a log line represents a distribution event for the given node
// and extracts the timestamp and filename.
func parseDistributorLine(line, node string, loc *time.Location) (time.Time, string, bool) {
	if !strings.Contains(line, "["+node+"]") && !strings.Contains(line, "["+node+".") {
		return time.Time{}, "", false
	}
	idx := strings.Index(line, "Distributed file")
	if idx == -1 {
		return time.Time{}, "", false
	}

	// Extract filename after "Distributed file"
	rest := strings.TrimSpace(line[idx+len("Distributed file"):])
	if strings.HasPrefix(rest, ":") {
		rest = strings.TrimSpace(rest[1:])
	}
	fileFields := strings.Fields(rest)
	if len(fileFields) == 0 {
		return time.Time{}, "", false
	}
	lastFile := filepath.Base(fileFields[0])

	// Extract timestamp from line prefix (fields 0 and 1: date and time)
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return time.Time{}, "", false
	}

	dateStr := fields[0]
	timeStr := fields[1]

	// Strip subseconds if present (e.g. 103000.123 -> 103000, 10:30:00.123 -> 10:30:00)
	if dotIdx := strings.Index(timeStr, "."); dotIdx != -1 {
		timeStr = timeStr[:dotIdx]
	}

	// If time is 6 digits without colons (e.g. 103000), format as 10:30:00
	if len(timeStr) == 6 && !strings.Contains(timeStr, ":") {
		timeStr = timeStr[0:2] + ":" + timeStr[2:4] + ":" + timeStr[4:6]
	}

	if loc == nil {
		loc = time.Local
	}

	layouts := []string{
		"20060102 15:04:05",
		"2006-01-02 15:04:05",
		"2006/01/02 15:04:05",
		"20060102 150405",
	}

	for _, layout := range layouts {
		if t, err := time.ParseInLocation(layout, dateStr+" "+timeStr, loc); err == nil {
			return t, lastFile, true
		}
	}

	return time.Time{}, "", false
}

// ScanDistributorActivity searches trace logs for the latest distribution entry for a node.
// Searches day by day backwards from now up to lookbackDays.
func ScanDistributorActivity(tracelogDir, baseLabel, stream, node string, lookbackDays int, minDelay time.Duration, now time.Time) (NodeActivity, error) {
	if tracelogDir == "" || lookbackDays <= 0 {
		return NodeActivity{
			Base:     baseLabel,
			Stream:   stream,
			Node:     node,
			NodeType: "distributor",
			LastFile: "-",
			Success:  false,
		}, nil
	}

	var latestTime time.Time
	var latestFile string
	found := false

	loc := now.Location()

	for d := 0; d < lookbackDays; d++ {
		dateStr := now.AddDate(0, 0, -d).Format("20060102")
		pattern := filepath.Join(tracelogDir, fmt.Sprintf("execution_trace_%s_%s_*", stream, dateStr))
		matches, err := filepath.Glob(pattern)
		if err != nil || len(matches) == 0 {
			continue
		}

		// Sort trace files in reverse so we scan the newest trace files first
		sort.Sort(sort.Reverse(sort.StringSlice(matches)))

		for _, tracePath := range matches {
			f, err := os.Open(tracePath)
			if err != nil {
				continue
			}

			scanner := bufio.NewScanner(f)
			for scanner.Scan() {
				line := scanner.Text()
				if t, file, ok := parseDistributorLine(line, node, loc); ok {
					if !found || t.After(latestTime) {
						latestTime = t
						latestFile = file
						found = true
					}
				}
			}
			f.Close()

			if found {
				break
			}
		}

		if found {
			// Found the latest distribution on day d — stop looking further back.
			break
		}
	}

	if !found {
		return NodeActivity{
			Base:     baseLabel,
			Stream:   stream,
			Node:     node,
			NodeType: "distributor",
			LastFile: "-",
			Success:  false,
		}, nil
	}

	epoch := latestTime.Unix()
	delay := now.Unix() - epoch
	if delay < 0 {
		delay = 0
	}
	if minDelay > 0 && delay <= int64(minDelay.Seconds()) {
		delay = 0
	}

	return NodeActivity{
		Base:                  baseLabel,
		Stream:                stream,
		Node:                  node,
		NodeType:              "distributor",
		LastActivityTimestamp: float64(epoch),
		DelaySec:              float64(delay),
		LastFile:              latestFile,
		Success:               true,
	}, nil
}

var nodeTypeRegex = regexp.MustCompile(`NodeType\s+"([^"]+)"`)

// detectNodeType attempts to read control/1/config or deduce the node type.
func detectNodeType(nodeDir, nodeName string) string {
	configPath := filepath.Join(nodeDir, "control", "1", "config")
	if data, err := os.ReadFile(configPath); err == nil {
		if m := nodeTypeRegex.FindStringSubmatch(string(data)); len(m) > 1 {
			t := strings.ToLower(m[1])
			if strings.Contains(t, "collector") || strings.Contains(t, "col") {
				return "collector"
			}
			if strings.Contains(t, "distributor") || strings.Contains(t, "dis") {
				return "distributor"
			}
		}
	}

	// Fallback to storage/1/audit_info presence
	auditPath := filepath.Join(nodeDir, "storage", "1", "audit_info")
	if _, err := os.Stat(auditPath); err == nil {
		return "collector"
	}

	lower := strings.ToLower(nodeName)
	if strings.Contains(lower, "col") {
		return "collector"
	}
	if strings.Contains(lower, "dis") {
		return "distributor"
	}

	return ""
}

// DiscoverStreamNodes discovers stream node directories under configured targets.
func DiscoverStreamNodes(targets []Target) []StreamNode {
	var nodes []StreamNode
	seen := make(map[string]bool)

	for _, t := range targets {
		base := filepath.Clean(t.Base)
		baseLabel := t.Label
		if baseLabel == "" {
			baseLabel = base
		}

		entries, err := os.ReadDir(base)
		if err != nil {
			continue
		}

		for _, streamEntry := range entries {
			if !streamEntry.IsDir() {
				continue
			}
			streamName := streamEntry.Name()
			nodesDir := filepath.Join(base, streamName, "nodes")
			info, err := os.Stat(nodesDir)
			if err != nil || !info.IsDir() {
				continue
			}

			nodeEntries, err := os.ReadDir(nodesDir)
			if err != nil {
				continue
			}

			for _, ne := range nodeEntries {
				if !ne.IsDir() {
					continue
				}
				nodeName := ne.Name()
				if strings.HasPrefix(nodeName, "mail") || strings.HasPrefix(nodeName, "rap_import") {
					continue
				}

				nodeDir := filepath.Join(nodesDir, nodeName)
				key := baseLabel + "/" + streamName + "/" + nodeName
				if seen[key] {
					continue
				}
				seen[key] = true

				nodeType := detectNodeType(nodeDir, nodeName)
				nodes = append(nodes, StreamNode{
					Base:     baseLabel,
					Stream:   streamName,
					Node:     nodeName,
					NodeDir:  nodeDir,
					NodeType: nodeType,
				})
			}
		}
	}

	return nodes
}

// IsNodeStopped checks whether the heartbeat file ${nodeDir}/control/1/heartbeat exists.
// It replicates the legacy shell test: [ ! -f "${node}/control/1/heartbeat" ].
// Returns true (stopped = 1) if the heartbeat file is missing or is not a regular file.
func IsNodeStopped(nodeDir string) bool {
	heartbeatPath := filepath.Join(nodeDir, "control", "1", "heartbeat")
	info, err := os.Stat(heartbeatPath)
	if err != nil {
		return true
	}
	return !info.Mode().IsRegular()
}

// ScanAllNodeStoppedStatuses checks every discovered node for heartbeat file presence.
// For each node it sets Stopped=true when the heartbeat file is missing, otherwise false.
// This mirrors the legacy STRAMES_DETAIL[$i, $STOPPED_INDEX]=1 assignment.
func ScanAllNodeStoppedStatuses(nodes []StreamNode) map[string]NodeStoppedStatus {
	results := make(map[string]NodeStoppedStatus, len(nodes))
	for _, n := range nodes {
		stopped := IsNodeStopped(n.NodeDir)
		key := n.Base + "/" + n.Stream + "/" + n.Node
		// Include NodeType in the key only if it would not collide; keep base/stream/node as primary key
		// and store the full entry. Use same separator scheme as activities for consistency.
		// To avoid collisions when two bases share stream/node names, the base prefix ensures uniqueness.
		// For map key, include NodeType as suffix if present to preserve distinction (collector vs distributor
		// naming collisions are unlikely, but keep it deterministic).
		if n.NodeType != "" {
			key = key + "/" + n.NodeType
		}
		results[key] = NodeStoppedStatus{
			Base:     n.Base,
			Stream:   n.Stream,
			Node:     n.Node,
			NodeType: n.NodeType,
			Stopped:  stopped,
		}
	}
	return results
}

// parseTraceTimestamp extracts the leading date/time fields from a trace log line
// and converts them to a Unix timestamp. Expected prefix: "YYYYMMDD HHMMSS[.ms] ..."
// or "YYYY-MM-DD HH:MM:SS[.ms] ...". Returns 0 if parsing fails.
func parseTraceTimestamp(line string, loc *time.Location) float64 {
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return 0
	}
	dateStr := fields[0]
	timeStr := fields[1]
	if dotIdx := strings.Index(timeStr, "."); dotIdx != -1 {
		timeStr = timeStr[:dotIdx]
	}
	if len(timeStr) == 6 && !strings.Contains(timeStr, ":") {
		timeStr = timeStr[0:2] + ":" + timeStr[2:4] + ":" + timeStr[4:6]
	}
	if loc == nil {
		loc = time.Local
	}
	layouts := []string{
		"20060102 15:04:05",
		"2006-01-02 15:04:05",
		"2006/01/02 15:04:05",
		"20060102 150405",
	}
	for _, layout := range layouts {
		if t, err := time.ParseInLocation(layout, dateStr+" "+timeStr, loc); err == nil {
			return float64(t.Unix())
		}
	}
	return 0
}

// isNodeFailedInLogs searches TRACEDIR/execution_trace_<STREAM>_<DATE>_* files for
// the failure pattern:  \[NODE\] .* (aborted and was disabled|Node index .* failed)
// This mirrors: grep -nE "\[${NODE}\] .* (aborted and was disabled|Node index .* failed)"
func isNodeFailedInLogs(tracelogDir, stream, node string, lookbackDays int, now time.Time) (bool, float64) {
	if tracelogDir == "" || lookbackDays <= 0 {
		return false, 0
	}
	// Build regex: \[NODE\] .* (aborted and was disabled|Node index .* failed)
	pattern := `\[` + regexp.QuoteMeta(node) + `\] .* (aborted and was disabled|Node index .* failed)`
	re, err := regexp.Compile(pattern)
	if err != nil {
		return false, 0
	}
	loc := now.Location()
	found := false
	var lastTs float64
	var latestTime float64
	for d := 0; d < lookbackDays; d++ {
		dateStr := now.AddDate(0, 0, -d).Format("20060102")
		globPat := filepath.Join(tracelogDir, fmt.Sprintf("execution_trace_%s_%s_*", stream, dateStr))
		matches, err := filepath.Glob(globPat)
		if err != nil || len(matches) == 0 {
			continue
		}
		sort.Strings(matches)
		for _, tracePath := range matches {
			f, err := os.Open(tracePath)
			if err != nil {
				continue
			}
			scanner := bufio.NewScanner(f)
			// Allow long lines (trace logs can be verbose)
			buf := make([]byte, 0, 64*1024)
			scanner.Buffer(buf, 1024*1024)
			for scanner.Scan() {
				line := scanner.Text()
				if re.MatchString(line) {
					found = true
					ts := parseTraceTimestamp(line, loc)
					// Keep the latest timestamp (max) to emulate tail -1 picking last match
					if ts > latestTime {
						latestTime = ts
					}
					lastTs = ts
				}
			}
			f.Close()
		}
		// If we found a match on the most recent day (d==0), prefer its latest timestamp
		// but continue scanning older days only if no match yet, to keep behavior close to
		// legacy (which only checks trace_date). For lookback, any match within window
		// counts as failed, and we report the most recent timestamp.
		if found && d == 0 && latestTime > 0 {
			// Already have a recent failure; still check if there's a later timestamp
			// within same day's remaining files – already captured. Break early for
			// efficiency if we want lookback to be inclusive: keep scanning older days
			// only to find any failure, but latestTime already is from most recent day
			// so we can return immediately.
			return true, latestTime
		}
		if found {
			// Found in older day; continue to check more recent days? Actually we iterate
			// from most recent to oldest, so if we are here at d>0, we have already
			// checked newer days and found nothing, so this is the most recent available.
			return true, lastTs
		}
	}
	if found {
		return true, lastTs
	}
	return false, 0
}

// ScanAllNodeFailedStatuses identifies failed nodes by trace-log search + stopped check.
// A node is marked Failed=true only when BOTH conditions hold:
//  1) A matching failure line is found in TRACEDIR/execution_trace_<STREAM>_<DATE>_*
//  2) The node is currently stopped (heartbeat file missing).
// This replicates the legacy condition: error_line != "" && STOPPED_INDEX==1 => FAILED_INDEX=1.
func ScanAllNodeFailedStatuses(nodes []StreamNode, tracelogDir string, lookbackDays int, now time.Time) map[string]NodeFailedStatus {
	results := make(map[string]NodeFailedStatus, len(nodes))
	for _, n := range nodes {
		stopped := IsNodeStopped(n.NodeDir)
		failed := false
		var failedTime float64
		if stopped {
			ok, ts := isNodeFailedInLogs(tracelogDir, n.Stream, n.Node, lookbackDays, now)
			failed = ok
			failedTime = ts
		}
		key := n.Base + "/" + n.Stream + "/" + n.Node
		if n.NodeType != "" {
			key = key + "/" + n.NodeType
		}
		results[key] = NodeFailedStatus{
			Base:       n.Base,
			Stream:     n.Stream,
			Node:       n.Node,
			NodeType:   n.NodeType,
			Failed:     failed,
			FailedTime: failedTime,
		}
	}
	return results
}

// ScanAllNodeActivities scans all discovered nodes for collector and distributor activities.
func ScanAllNodeActivities(ctx context.Context, nodes []StreamNode, tracelogDir string, minDelay time.Duration, lookbackDays int, workers int, now time.Time) map[string]NodeActivity {
	if len(nodes) == 0 {
		return make(map[string]NodeActivity)
	}

	if workers <= 0 {
		workers = 2
	}

	results := make(map[string]NodeActivity, len(nodes))
	var mu sync.Mutex

	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup

	for _, n := range nodes {
		if ctx.Err() != nil {
			break
		}

		wg.Add(1)
		sem <- struct{}{}

		go func(sn StreamNode) {
			defer wg.Done()
			defer func() { <-sem }()

			var act NodeActivity
			var err error

			switch sn.NodeType {
			case "collector":
				act, err = ScanCollectorActivity(sn.Base, sn.Stream, sn.Node, sn.NodeDir, minDelay, now)
			case "distributor":
				act, err = ScanDistributorActivity(tracelogDir, sn.Base, sn.Stream, sn.Node, lookbackDays, minDelay, now)
			default:
				// If unspecified, test for audit_info first
				auditPath := filepath.Join(sn.NodeDir, "storage", "1", "audit_info")
				if _, statErr := os.Stat(auditPath); statErr == nil {
					act, err = ScanCollectorActivity(sn.Base, sn.Stream, sn.Node, sn.NodeDir, minDelay, now)
				} else if tracelogDir != "" {
					act, err = ScanDistributorActivity(tracelogDir, sn.Base, sn.Stream, sn.Node, lookbackDays, minDelay, now)
				}
			}

			if err == nil && act.Success {
				key := act.Base + "/" + act.Stream + "/" + act.Node + "/" + act.NodeType
				mu.Lock()
				results[key] = act
				mu.Unlock()
			}
		}(n)
	}

	wg.Wait()
	return results
}
