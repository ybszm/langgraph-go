package docs_test

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var markdownLink = regexp.MustCompile(`\[[^]]*\]\(([^)]+)\)`)

func TestLocalMarkdownLinksAndVersionClaims(t *testing.T) {
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	paths, err := publicMarkdownFiles(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		text := string(data)
		if strings.Contains(text, "Go 1.24") {
			t.Errorf("%s still claims Go 1.24", path)
		}
		if strings.Contains(text, "New(prebuilt.AgentState)(") {
			t.Errorf("%s contains an invalid generic provider constructor", path)
		}
		inFence := false
		for _, line := range strings.Split(text, "\n") {
			if strings.HasPrefix(strings.TrimSpace(line), "```") {
				inFence = !inFence
				continue
			}
			if inFence {
				continue
			}
			for _, match := range markdownLink.FindAllStringSubmatch(line, -1) {
				target := strings.TrimSpace(strings.Split(match[1], " ")[0])
				target = strings.Trim(target, "<>")
				if target == "" || strings.HasPrefix(target, "#") || strings.Contains(target, "://") || strings.HasPrefix(target, "mailto:") {
					continue
				}
				target = strings.Split(target, "#")[0]
				if target == "" {
					continue
				}
				resolved := filepath.Clean(filepath.Join(filepath.Dir(path), filepath.FromSlash(target)))
				if _, err := os.Stat(resolved); err != nil {
					t.Errorf("%s links to missing %s", path, target)
				}
			}
		}
	}
}

func publicMarkdownFiles(root string) ([]string, error) {
	command := exec.Command("git", "-C", root, "ls-files", "-z", "--", "*.md")
	if output, err := command.Output(); err == nil {
		entries := bytes.Split(output, []byte{0})
		paths := make([]string, 0, len(entries))
		for _, entry := range entries {
			if len(entry) == 0 {
				continue
			}
			paths = append(paths, filepath.Join(root, filepath.FromSlash(string(entry))))
		}
		return paths, nil
	}

	var paths []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() && (entry.Name() == ".git" || entry.Name() == ".gocache") {
			return filepath.SkipDir
		}
		if !entry.IsDir() && filepath.Ext(path) == ".md" {
			paths = append(paths, path)
		}
		return nil
	})
	return paths, err
}
