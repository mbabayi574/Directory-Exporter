package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestExtractInDataPath_FilePrefix(t *testing.T) {
	cases := []struct{ in, want string }{
		{`InDataPath	"COLLECTED,file:/comptel/elink/install/buffer/401/COLLECTED_0_972"`, "/comptel/elink/install/buffer/401/COLLECTED_0_972"},
		{`InDataPath	"COLLECTED,file,copy:/comptel/elink/install/buffer/401/COLLECTED_1_1049"`, "/comptel/elink/install/buffer/401/COLLECTED_1_1049"},
		{`InDataPath: "/data/streams/input"`, "/data/streams/input"},
		{`InDataPath	"/data/streams/input"`, "/data/streams/input"},
	}
	for _, tc := range cases {
		if got := extractInDataPath(tc.in); got != tc.want {
			t.Errorf("extractInDataPath(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestResolveNodeBufferPaths_FilePrefixed(t *testing.T) {
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "control", "1"), 0755)
	cfg := "NodeType \"distributor\"\nInDataPath\t\"COLLECTED,file:/comptel/elink/install/buffer/401/COLLECTED_0_972\"\n"
	os.WriteFile(filepath.Join(dir, "control", "1", "config"), []byte(cfg), 0644)

	nt, paths := resolveNodeBufferPaths(dir)
	if nt != "distributor" {
		t.Errorf("nodeType=%q want distributor", nt)
	}
	if len(paths) != 1 || paths[0] != "/comptel/elink/install/buffer/401/COLLECTED_0_972" {
		t.Errorf("paths=%v want [/comptel/elink/install/buffer/401/COLLECTED_0_972]", paths)
	}
}

// When config-resolved buffer paths don't exist locally, the buffer falls
// back to aggregating watched directory_file_count values under the node's
// local input/ subtree (regression test for Back_Dump reporting 0).
func TestScanAllNodeBuffers_FallsBackToLocalInput(t *testing.T) {
	base := t.TempDir()
	nodeDir := filepath.Join(base, "CBSDump", "nodes", "Back_Dump")
	for _, sub := range []string{"input/COLLECTED_1_968", "input/COLLECTED_2_1058"} {
		os.MkdirAll(filepath.Join(nodeDir, sub), 0755)
	}
	os.WriteFile(filepath.Join(nodeDir, "input", "COLLECTED_1_968", "a.dat"), []byte("x"), 0644)
	os.WriteFile(filepath.Join(nodeDir, "input", "COLLECTED_1_968", "b.dat"), []byte("x"), 0644)
	os.WriteFile(filepath.Join(nodeDir, "input", "COLLECTED_2_1058", "c.dat"), []byte("x"), 0644)
	os.MkdirAll(filepath.Join(nodeDir, "control", "1"), 0755)
	os.WriteFile(filepath.Join(nodeDir, "control", "1", "config"),
		[]byte("NodeType \"distributor\"\nInDataPath\t\"COLLECTED,file:/comptel/elink/install/buffer/401/COLLECTED_0_972\"\n"), 0644)

	nodes := []StreamNode{{Base: "/streams", Stream: "CBSDump", Node: "Back_Dump", NodeDir: nodeDir, NodeType: "distributor"}}
	results := map[string]DirMetrics{
		filepath.Join(base, "CBSDump", "nodes", "Back_Dump", "input", "COLLECTED_1_968"): {FileCount: 2, ScanSuccess: 1},
		filepath.Join(base, "CBSDump", "nodes", "Back_Dump", "input", "COLLECTED_2_1058"): {FileCount: 1, ScanSuccess: 1},
	}
	buffers := ScanAllNodeBuffers(context.Background(), nodes, results, nil, 2)
	nb, ok := buffers["/streams/CBSDump/Back_Dump/distributor"]
	if !ok {
		t.Fatalf("missing buffer entry in %v", buffers)
	}
	if nb.FileCount != 3 || !nb.Success {
		t.Errorf("got count=%d success=%v want 3,true (BufferPath=%q)", nb.FileCount, nb.Success, nb.BufferPath)
	}
}

// Config-resolved paths that exist keep priority over the input/ fallback.
func TestScanAllNodeBuffers_ConfigPathWins(t *testing.T) {
	extDir := t.TempDir()
	os.WriteFile(filepath.Join(extDir, "f.dat"), []byte("x"), 0644)
	nodeDir := t.TempDir()
	os.MkdirAll(filepath.Join(nodeDir, "control", "1"), 0755)
	os.WriteFile(filepath.Join(nodeDir, "control", "1", "config"),
		[]byte("NodeType \"decoder\"\nInDataPath: \""+extDir+"\"\n"), 0644)

	nodes := []StreamNode{{Base: "/streams", Stream: "S", Node: "n1", NodeDir: nodeDir, NodeType: "decoder"}}
	buffers := ScanAllNodeBuffers(context.Background(), nodes, map[string]DirMetrics{}, nil, 2)
	nb := buffers["/streams/S/n1/decoder"]
	if nb.FileCount != 1 || !nb.Success {
		t.Errorf("got count=%d success=%v want 1,true", nb.FileCount, nb.Success)
	}
}

func TestRenderMetrics_BufferFiles(t *testing.T) {
	snap := CacheSnapshot{
		Ready:        true,
		LastScanTime: time.Now(),
		NodeBuffers: []NodeBuffer{
			{Base: "/streams", Stream: "CBSDump", Node: "Back_Dump", NodeType: "distributor", BufferPath: "/x", FileCount: 138, Success: true},
		},
	}
	var buf bytes.Buffer
	RenderMetrics(&buf, snap)
	if !strings.Contains(buf.String(), `directory_buffer_files{base="/streams",stream="CBSDump",node="Back_Dump",type="distributor"} 138`) {
		t.Errorf("missing directory_buffer_files line:\n%s", buf.String())
	}
}
