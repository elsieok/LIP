package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

// llamaClient talks to llama.cpp's llama-server (OpenAI-compatible API).
type llamaClient struct {
	base string
	hc   *http.Client
}

func newLlamaClient(base string) *llamaClient {
	return &llamaClient{
		base: strings.TrimRight(base, "/"),
		// No overall Timeout: responses are streamed and can run for a while.
		hc: &http.Client{Transport: &http.Transport{
			MaxIdleConnsPerHost: 16,
			IdleConnTimeout:     90 * time.Second,
		}},
	}
}

// healthy is true when llama-server has loaded the model (GET /health -> 200;
// it returns 503 while loading).
func (c *llamaClient) healthy(ctx context.Context) bool {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/health", nil)
	if err != nil {
		return false
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode == http.StatusOK
}

func (c *llamaClient) waitReady(ctx context.Context) error {
	for i := 0; ; i++ {
		if c.healthy(ctx) {
			return nil
		}
		if i%10 == 9 {
			log.Printf("still waiting for llama-server at %s ...", c.base)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

type chatChunk struct {
	Choices []struct {
		Delta struct {
			Content string `json:"content"`
		} `json:"delta"`
	} `json:"choices"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error"`
}

// stream sends one chat completion and calls onToken for every non-empty
// content delta, in order.
func (c *llamaClient) stream(ctx context.Context, prompt string, maxTokens int, onToken func(string)) error {
	body, err := json.Marshal(map[string]any{
		"messages":   []map[string]string{{"role": "user", "content": prompt}},
		"max_tokens": maxTokens,
		"stream":     true,
		// Match llama-cpp-python's create_chat_completion defaults so the
		// Python and Go workers generate comparable output for benchmarks.
		"temperature": 0.2,
		"top_p":       0.95,
		"top_k":       40,
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("llama-server returned %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}

	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if data == "[DONE]" {
			return nil
		}
		var ch chatChunk
		if err := json.Unmarshal([]byte(data), &ch); err != nil {
			return fmt.Errorf("bad chunk from llama-server: %w", err)
		}
		if ch.Error != nil {
			return fmt.Errorf("llama-server error: %s", ch.Error.Message)
		}
		if len(ch.Choices) > 0 && ch.Choices[0].Delta.Content != "" {
			onToken(ch.Choices[0].Delta.Content)
		}
	}
	return sc.Err()
}
