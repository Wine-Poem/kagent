// Command echollm is an OpenAI-compatible model that reports what history it received. It lets
// a probe see whether a restored or forked session still carries the conversation.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
)

type chatRequest struct {
	Messages []struct {
		Role    string          `json:"role"`
		Content json.RawMessage `json:"content"`
	} `json:"messages"`
}

func main() {
	listen := flag.String("listen", ":8080", "listen address")
	token := flag.String("token", "ZEBRA-4711", "word whose presence in the history is reported")
	flag.Parse()
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		var req chatRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var all strings.Builder
		turns := 0
		for _, m := range req.Messages {
			all.Write(m.Content)
			if m.Role == "user" {
				turns++
			}
		}
		reply := fmt.Sprintf("user-turns=%d %s=%t", turns, *token, strings.Contains(all.String(), *token))
		slog.Info("chat", "reply", reply)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id": "chatcmpl-echo", "object": "chat.completion", "created": 1, "model": "echo-model",
			"choices": []map[string]any{{"index": 0, "finish_reason": "stop", "message": map[string]any{"role": "assistant", "content": reply}}},
		})
	})
	if err := http.ListenAndServe(*listen, mux); err != nil {
		slog.Error("echollm stopped", "error", err)
		os.Exit(1)
	}
}
