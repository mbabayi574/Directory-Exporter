package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// WatchEntry pairs an absolute directory path with its pre-computed Prometheus labels.
type WatchEntry struct {
	AbsPath string
	Labels  DirLabels
}

// WatchList is a thread-safe, copy-on-write list of directories to monitor.
type WatchList struct {
	mu      sync.RWMutex
	entries []WatchEntry
}

func NewWatchList() *WatchList { return &WatchList{} }

func (wl *WatchList) Set(entries []WatchEntry) {
	wl.mu.Lock()
	wl.entries = entries
	wl.mu.Unlock()
}

func (wl *WatchList) Get() []WatchEntry {
	wl.mu.RLock()
	cp := make([]WatchEntry, len(wl.entries))
	copy(cp, wl.entries)
	wl.mu.RUnlock()
	return cp
}

func (wl *WatchList) Len() int {
	wl.mu.RLock()
	n := len(wl.entries)
	wl.mu.RUnlock()
	return n
}

// discoverTargets builds the full watch list from all configured targets.
// For each target:
//   - Dirs non-empty → use exactly those subdirs (relative to Base), no filesystem walk.
//   - Dirs empty     → auto-discover subdirectories up to MaxDepth via WalkDir.
//                    If Pattern is set, only matching directories are kept.
func discoverTargets(targets []Target) ([]WatchEntry, error) {
	var all []WatchEntry
	var firstErr error
	for _, t := range targets {
		entries, err := discoverTarget(t)
		if err != nil && firstErr == nil {
			firstErr = err
		}
		all = append(all, entries...)
	}
	return all, firstErr
}

func discoverTarget(t Target) ([]WatchEntry, error) {
	base := filepath.Clean(t.Base)
	label := t.Label
	if label == "" {
		label = base
	}
	if len(t.Dirs) > 0 {
		return resolveExplicitDirs(base, label, t.Dirs)
	}
	if t.Pattern != "" {
		if _, err := filepath.Match(t.Pattern, "test"); err != nil {
			return nil, fmt.Errorf("invalid pattern %q: %w", t.Pattern, err)
		}
	}
	maxDepth := t.MaxDepth
	if maxDepth == 0 {
		if t.Pattern != "" {
			maxDepth = len(strings.Split(t.Pattern, string(os.PathSeparator)))
		} else {
			maxDepth = 1
		}
	}
	return discoverDirectories(base, label, maxDepth, t.Pattern)
}

// resolveExplicitDirs turns a list of relative dir paths into WatchEntries
// without touching the filesystem. Missing directories are caught at scan time
// via scrape_success=0.
func resolveExplicitDirs(base, label string, dirs []string) ([]WatchEntry, error) {
	var entries []WatchEntry
	seen := make(map[string]bool, len(dirs))
	for _, d := range dirs {
		rel := filepath.Clean(d)
		if seen[rel] {
			continue
		}
		seen[rel] = true

		absPath := filepath.Join(base, rel)
		parts := strings.Split(rel, string(os.PathSeparator))

		labels := DirLabels{Base: label}
		switch len(parts) {
		case 1:
			labels.Stream = parts[0]
		default:
			labels.Stream = parts[0]
			labels.Type = strings.Join(parts[1:], "/")
		}
		entries = append(entries, WatchEntry{AbsPath: absPath, Labels: labels})
	}
	return entries, nil
}

// validateTargets checks that every configured base path exists and is readable.
// Returns a multi-error string listing all failures; returns nil when all are OK.
func validateTargets(targets []Target) error {
	var errs []string
	for _, t := range targets {
		base := filepath.Clean(t.Base)
		info, err := os.Stat(base)
		if err != nil {
			errs = append(errs, fmt.Sprintf("  base %q: %v", base, err))
			continue
		}
		if !info.IsDir() {
			errs = append(errs, fmt.Sprintf("  base %q: not a directory", base))
			continue
		}
		f, err := os.Open(base)
		if err != nil {
			errs = append(errs, fmt.Sprintf("  base %q: cannot open: %v", base, err))
			continue
		}
		f.Close()

		// Validate explicit dirs exist too
		for _, d := range t.Dirs {
			abs := filepath.Join(base, filepath.Clean(d))
			if _, err := os.Stat(abs); err != nil {
				errs = append(errs, fmt.Sprintf("  target %q: %v", abs, err))
			}
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("target validation errors:\n%s", strings.Join(errs, "\n"))
}

// discoverDirectories walks basePath up to maxDepth and returns one WatchEntry
// per subdirectory. This is used for auto-discovery when Dirs is not specified.
//
// If pattern is non-empty, only directories whose relative path matches the glob
// pattern are added to the watch list. A * in the pattern matches one path segment.
//
// Label derivation:
//
//	depth 1: stream=<dir>,        type=""
//	depth 2: stream=<dir>,        type=<subdir>
//	depth N: stream=<first-part>, type=<rest joined with "/">
func discoverDirectories(basePath, label string, maxDepth int, pattern string) ([]WatchEntry, error) {
	basePath = filepath.Clean(basePath)
	var entries []WatchEntry
	var firstErr error

	walkErr := filepath.WalkDir(basePath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			return nil
		}
		if path == basePath || !d.IsDir() {
			return nil
		}

		rel, relErr := filepath.Rel(basePath, path)
		if relErr != nil {
			return nil
		}

		parts := strings.Split(rel, string(os.PathSeparator))
		depth := len(parts)

		if depth > maxDepth {
			return filepath.SkipDir
		}

		if pattern != "" {
			matched, matchErr := filepath.Match(pattern, rel)
			if matchErr != nil {
				if firstErr == nil {
					firstErr = matchErr
				}
				return nil
			}
			if !matched {
				return nil
			}
		}

		labels := DirLabels{Base: label}
		switch {
		case depth == 1:
			labels.Stream = parts[0]
		default:
			labels.Stream = parts[0]
			labels.Type = strings.Join(parts[1:], "/")
		}

		entries = append(entries, WatchEntry{AbsPath: path, Labels: labels})
		return nil
	})

	if walkErr != nil && firstErr == nil {
		firstErr = walkErr
	}
	return entries, firstErr
}
