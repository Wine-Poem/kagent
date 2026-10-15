package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
)

// sandboxPerf times the standalone sandbox lifecycle through the kagent CLI, the way a user
// drives it: create (retrying with the same request ID as the CLI asks), exec, file transfer,
// suspend, resume and delete. Driving the CLI keeps the client identical on both platforms.
func sandboxPerf(cli, apiURL, template string, n int) {
	rec := &recorder{samples: map[string][]time.Duration{}, notes: map[string]string{}}
	run := func(args ...string) (string, error) {
		// Global flags go first: anything after "--" belongs to the remote command.
		cmd := exec.Command(cli, append([]string{"-n", "kagent", "--api-url", apiURL, "--gateway-url", apiURL}, args...)...)
		var out bytes.Buffer
		cmd.Stdout, cmd.Stderr = &out, &out
		err := cmd.Run()
		return out.String(), err
	}
	dir, err := os.MkdirTemp("", "sandboxperf")
	check(err)
	defer os.RemoveAll(dir)
	payload := filepath.Join(dir, "payload.bin")
	check(os.WriteFile(payload, bytes.Repeat([]byte("kagent-perf-"), 1<<20/12), 0o644))
	for i := 0; i < n; i++ {
		requestID := uuid.NewString()
		var id string
		attempts := 0
		created := timed(func() {
			for attempts < 6 {
				attempts++
				out, err := run("sandbox", "create", template, "--request-id", requestID, "--name", fmt.Sprintf("perf-%d", i), "-o", "json")
				if err == nil {
					var parsed struct {
						ID string `json:"id"`
					}
					if json.NewDecoder(strings.NewReader(firstJSON(out))).Decode(&parsed) == nil && parsed.ID != "" {
						id = parsed.ID
						return
					}
				}
				time.Sleep(2 * time.Second)
			}
		})
		if id == "" {
			rec.note("sandbox.create", "create failed after retries")
			continue
		}
		rec.add("sandbox.create", created)
		rec.add("sandbox.create_attempts_x1000ms", time.Duration(attempts)*time.Second)
		exe := func(name string, command ...string) {
			d := timed(func() {
				if out, err := run(append([]string{"sandbox", "exec", id, "--"}, command...)...); err != nil {
					rec.note(name, strings.TrimSpace(lastLine(out)))
				}
			})
			rec.add(name, d)
		}
		exe("sandbox.exec_first", "sh", "-c", "echo first")
		for j := 0; j < 3; j++ {
			exe("sandbox.exec", "sh", "-c", "echo again")
		}
		step := func(name string, args ...string) {
			d := timed(func() {
				if out, err := run(args...); err != nil {
					rec.note(name, strings.TrimSpace(lastLine(out)))
				}
			})
			rec.add(name, d)
		}
		step("sandbox.upload_1MiB", "sandbox", "upload", id, payload, "/data/workspace/payload.bin")
		step("sandbox.download_1MiB", "sandbox", "download", id, "/data/workspace/payload.bin", filepath.Join(dir, "back.bin"))
		step("sandbox.suspend", "sandbox", "suspend", id)
		rec.add("sandbox.resume", timed(func() {
			for k := 0; k < 6; k++ {
				if out, err := run("sandbox", "resume", id); err == nil && strings.Contains(out, "READY") {
					return
				}
				time.Sleep(2 * time.Second)
			}
		}))
		exe("sandbox.exec_after_resume", "sh", "-c", "test -f /data/workspace/payload.bin && echo kept")
		step("sandbox.delete", "sandbox", "delete", id)
	}
	rec.print()
}

func firstJSON(out string) string {
	if i := strings.Index(out, "{"); i >= 0 {
		return out[i:]
	}
	return out
}

func lastLine(out string) string {
	lines := strings.Split(strings.TrimSpace(out), "\n")
	return lines[len(lines)-1]
}
