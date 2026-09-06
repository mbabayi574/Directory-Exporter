package main

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
)

// NodeBuffer holds the input-buffer backlog for a single stream node.
// It mirrors monitoring.sh BUFFER_INDEX (STRAMES_DETAIL[$i, $BUFFER_INDEX]):
// the non-recursive regular-file count of the node's resolved input directory.
type NodeBuffer struct {
	Base       string
	Stream     string
	Node       string
	NodeType   string // "collector", "distributor", or "" when unknown
	BufferPath string // resolved input directory ("-" when unresolved/DATA_STORAGE)
	FileCount  int64
	Success    bool // true when the buffer dir was readable and counted
}

var (
	quotedValueRegex = regexp.MustCompile(`"([^"]+)"`)
)

// resolveNodeBufferPaths parses ${nodeDir}/control/1/config and returns the
// node type plus every input buffer directory found.
//
// Mirrors monitoring.sh get_streams_detail (lines 160-254):
//   - collector nodes: SourceDirectory "..." (or DATA_STORAGE sentinel)
//   - general nodes:   InDataPath ... "..."
//   - database loaders: ${ElrHome}/upload/${DatabaseTable}/in/
//
// One config may yield multiple input paths (one per matching line, as the
// shell script appends a STRAMES_DETAIL row per match). Callers sum the counts.
func resolveNodeBufferPaths(nodeDir string) (string, []string) {
	data, err := os.ReadFile(filepath.Join(nodeDir, "control", "1", "config"))
	if err != nil {
		return "", nil
	}

	var nodeType string
	var elrHome, databaseTable string
	var sourceDir string
	var hasDataStorage bool
	var inDataPaths []string
	var hasDatabaseLoaderLine bool

	// First pass: collect raw values (order-independent, unlike the shell
	// script which is order-sensitive — this is more robust while preserving
	// the same priority rules below).
	lines := strings.Split(string(data), "\n")
	for _, raw := range lines {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "NodeType") {
			if m := quotedValueRegex.FindStringSubmatch(line); len(m) > 1 {
				nodeType = m[1]
			}
		} else if strings.HasPrefix(line, "ElrHome") {
			hasDatabaseLoaderLine = true
			if m := quotedValueRegex.FindStringSubmatch(line); len(m) > 1 {
				elrHome = m[1]
			}
		} else if strings.HasPrefix(line, "DatabaseTable") {
			if m := quotedValueRegex.FindStringSubmatch(line); len(m) > 1 {
				databaseTable = m[1]
			}
		}
		if strings.HasPrefix(line, "SourceDirectory") {
			if m := quotedValueRegex.FindStringSubmatch(line); len(m) > 1 {
				sourceDir = m[1]
			}
		}
		if strings.Contains(line, "DataStorage") {
			hasDataStorage = true
		}
		if strings.HasPrefix(line, "InDataPath") {
			if p := extractInDataPath(line); p != "" {
				inDataPaths = append(inDataPaths, p)
			}
		}
	}

	lowerType := strings.ToLower(nodeType)
	isCollector := strings.Contains(lowerType, "collector")

	if isCollector {
		// Collectors: SourceDirectory wins; DATA_STORAGE means "no
		// filesystem buffer" (legacy script records link_state=NOT_EXIST
		// and a backlog of 0).
		if sourceDir != "" {
			return nodeType, []string{sourceDir}
		}
		if hasDataStorage {
			return nodeType, nil
		}
		// Fall through to database-loader path if present.
		if hasDatabaseLoaderLine && elrHome != "" && databaseTable != "" {
			return nodeType, []string{filepath.Join(elrHome, "upload", databaseTable, "in")}
		}
		return nodeType, nil
	}

	// General nodes: InDataPath wins (may be multiple lines → multiple paths).
	if len(inDataPaths) > 0 {
		return nodeType, inDataPaths
	}
	if hasDatabaseLoaderLine && elrHome != "" && databaseTable != "" {
		return nodeType, []string{filepath.Join(elrHome, "upload", databaseTable, "in")}
	}
	return nodeType, nil
}

// extractInDataPath extracts the filesystem path from an InDataPath config
// line, mirroring monitoring.sh `cut -d':' -f2 | cut -d'"' -f1`.
//
// Real-world line formats:
//   - InDataPath "COLLECTED,file:/comptel/.../COLLECTED_0_972" → /comptel/.../COLLECTED_0_972
//   - InDataPath "COLLECTED,file,copy:/comptel/.../COLLECTED_1_968" → /comptel/.../COLLECTED_1_968
//   - InDataPath: "/data/streams/.../input" → /data/streams/.../input
func extractInDataPath(line string) string {
	quoted := ""
	if m := quotedValueRegex.FindStringSubmatch(line); len(m) > 1 {
		quoted = strings.TrimSpace(m[1])
	} else if idx := strings.Index(line, ":"); idx != -1 {
		quoted = strings.Trim(strings.TrimSpace(line[idx+1:]), `"'`)
		if f := strings.Fields(quoted); len(f) > 0 {
			quoted = f[0]
		}
	}
	if quoted == "" {
		return ""
	}
	// Strip the "<stream>,file[,...]:" transport prefix when present, e.g.
	// "COLLECTED,file:/path" and "COLLECTED,file,copy:/path" → "/path".
	// Plain paths (no file-transport marker) are returned as-is.
	if idx := strings.LastIndex(quoted, ":"); idx != -1 && strings.Contains(quoted[:idx], "file") {
		return strings.TrimSpace(quoted[idx+1:])
	}
	return quoted
}

// countBufferFiles counts regular files directly inside path (non-recursive),
// mirroring `find <dir> -maxdepth 1 -mindepth 1 -type f | wc -l` and the
// scanDir counting rules (regular files + symlinks resolving to files;
// subdirectories, symlinks to directories, and broken symlinks are skipped).
func countBufferFiles(ctx context.Context, path string) (int64, bool) {
	f, err := os.Open(path)
	if err != nil {
		return 0, false
	}
	defer f.Close()

	var count int64
	for {
		if ctx.Err() != nil {
			return count, false
		}
		entries, readErr := f.ReadDir(dirChunkSize)
		for _, e := range entries {
			if ctx.Err() != nil {
				return count, false
			}
			if e.Type()&os.ModeSymlink != 0 {
				info, statErr := os.Stat(filepath.Join(path, e.Name()))
				if statErr != nil || info.IsDir() {
					continue
				}
				count++
			} else if e.IsDir() {
				continue
			} else {
				count++
			}
		}
		if readErr != nil {
			break
		}
	}
	return count, true
}

// ScanAllNodeBuffers resolves each node's input buffer directory and counts
// files, reusing already-scanned directory_file_count values when the buffer
// path coincides with a watched directory (best performance: zero extra I/O
// for the common `*/nodes/*/[input|...]` watch pattern). Unwatched/external
// buffer paths (e.g. collector SourceDirectory outside the base) get a cheap
// count-only scan with no lstat timestamp work.
func ScanAllNodeBuffers(ctx context.Context, nodes []StreamNode, results map[string]DirMetrics, byPath map[string]string, workers int) map[string]NodeBuffer {
	_ = byPath // reserved for future path-alias support
	if len(nodes) == 0 {
		return make(map[string]NodeBuffer)
	}
	if workers <= 0 {
		workers = 2
	}

	// Fast lookup: cleaned buffer path -> cached file count (only for
	// successfully scanned watched dirs).
	cached := make(map[string]DirMetrics, len(results))
	for absPath, dm := range results {
		cached[filepath.Clean(absPath)] = dm
	}

	out := make(map[string]NodeBuffer, len(nodes))
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

			nodeType, paths := resolveNodeBufferPaths(sn.NodeDir)
			if nodeType == "" {
				nodeType = sn.NodeType
			}
			key := sn.Base + "/" + sn.Stream + "/" + sn.Node + "/" + nodeType

			var total int64
			okAll := true
			anyOK := false
			resolved := strings.Join(paths, ",")
			for _, p := range paths {
				clean := filepath.Clean(p)
				if dm, hit := cached[clean]; hit && dm.ScanSuccess == 1 {
					total += dm.FileCount
					anyOK = true
					continue
				}
				c, ok := countBufferFiles(ctx, clean)
				if !ok {
					okAll = false
					continue
				}
				total += c
				anyOK = true
			}

			if !anyOK {
				// No config-resolved buffer path was countable (typical when
				// the config references production paths like /comptel/...
				// that don't exist where the exporter runs). Fall back to
				// the node's local input/ subtree, reusing the already
				// scanned directory_file_count values: every watched dir at
				// or under <nodeDir>/input contributes its (non-recursive)
				// count, so the sum equals the recursive total under input/.
				inputDir := filepath.Clean(filepath.Join(sn.NodeDir, "input"))
				var fbTotal int64
				fbOK := false
				for absPath, dm := range cached {
					if dm.ScanSuccess != 1 {
						continue
					}
					if absPath == inputDir || strings.HasPrefix(absPath, inputDir+string(os.PathSeparator)) {
						fbTotal += dm.FileCount
						fbOK = true
					}
				}
				if fbOK {
					total = fbTotal
					okAll = true
					anyOK = true
					resolved = inputDir
				} else if _, statErr := os.Stat(inputDir); statErr == nil {
					// Watched nothing under input/ (e.g. discovery pattern
					// doesn't cover it) but the dir exists — count it
					// directly (non-recursive, like directory_file_count).
					if c, ok := countBufferFiles(ctx, inputDir); ok {
						total = c
						okAll = true
						anyOK = true
						resolved = inputDir
					}
				}
			}

			if !anyOK && len(paths) == 0 {
				resolved = "-"
			}

			mu.Lock()
			out[key] = NodeBuffer{
				Base: sn.Base, Stream: sn.Stream, Node: sn.Node,
				NodeType: nodeType, BufferPath: resolved,
				FileCount: total, Success: anyOK && okAll,
			}
			mu.Unlock()
		}(n)
	}
	wg.Wait()
	return out
}
