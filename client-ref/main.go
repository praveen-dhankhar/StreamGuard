package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

type frame struct {
	event string
	data  []byte
}

func main() {
	var endpoint string
	var apiKey string
	var noColor bool
	flag.StringVar(&endpoint, "endpoint", "http://localhost:8080/v1/stream", "StreamGuard /v1/stream URL")
	flag.StringVar(&apiKey, "api-key", "sg_live_demo", "client API key")
	flag.BoolVar(&noColor, "no-color", false, "disable ANSI color and faint output")
	flag.Parse()

	prompt := strings.Join(flag.Args(), " ")
	if prompt == "" {
		fmt.Print("prompt> ")
		line, _ := bufio.NewReader(os.Stdin).ReadString('\n')
		prompt = strings.TrimSpace(line)
	}
	body := map[string]any{
		"model":  "streamguard-demo-model",
		"stream": true,
		"messages": []map[string]string{{
			"role": "user", "content": prompt,
		}},
	}
	buf, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(buf))
	if err != nil {
		fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		fmt.Printf("request failed: %s %s\n", resp.Status, strings.TrimSpace(string(b)))
		os.Exit(1)
	}
	reader := bufio.NewReader(resp.Body)
	var dimmed bool
	for {
		fr, err := readFrame(reader)
		if err != nil {
			if err != io.EOF {
				fmt.Println("\nstream error:", err)
			}
			return
		}
		switch fr.event {
		case "gateway_status":
			var v struct {
				State    string `json:"state"`
				Provider string `json:"provider"`
			}
			_ = json.Unmarshal(fr.data, &v)
			fmt.Printf("%s provider=%s state=%s%s\n", badge("STATUS", noColor), v.Provider, v.State, reset(noColor))
		case "gateway_failover":
			var v struct {
				Reason       string `json:"reason"`
				ProviderTo   string `json:"provider_to"`
				ProviderFrom string `json:"provider_from"`
				Attempt      int    `json:"attempt"`
			}
			_ = json.Unmarshal(fr.data, &v)
			fmt.Printf("\n%s failover %d: %s -> %s reason=%s%s\n", badge("FAILOVER", noColor), v.Attempt, v.ProviderFrom, v.ProviderTo, v.Reason, reset(noColor))
		case "gateway_regenerating":
			dimmed = true
			fmt.Printf("%s retained partial remains visible; regenerated block starts below%s\n", faint(noColor), reset(noColor))
			fmt.Println("--- regenerated output ---")
		case "gateway_truncated":
			var v struct {
				Reason          string `json:"reason"`
				TokensDelivered int    `json:"tokens_delivered"`
			}
			_ = json.Unmarshal(fr.data, &v)
			fmt.Printf("\n%s truncated reason=%s tokens_delivered=%d%s\n", badge("TRUNCATED", noColor), v.Reason, v.TokensDelivered, reset(noColor))
			return
		default:
			text := extractText(fr.data)
			if dimmed {
				fmt.Print(reset(noColor))
				dimmed = false
			}
			fmt.Print(text)
		}
	}
}

func readFrame(r *bufio.Reader) (frame, error) {
	var lines []string
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return frame{}, err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			break
		}
		lines = append(lines, line)
	}
	var fr frame
	for _, line := range lines {
		if strings.HasPrefix(line, "event:") {
			fr.event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		}
		if strings.HasPrefix(line, "data:") {
			fr.data = []byte(strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		}
	}
	return fr, nil
}

func extractText(data []byte) string {
	var payload struct {
		Choices []struct {
			Delta struct {
				Content string `json:"content"`
			} `json:"delta"`
		} `json:"choices"`
	}
	if json.Unmarshal(data, &payload) != nil || len(payload.Choices) == 0 {
		return ""
	}
	return payload.Choices[0].Delta.Content
}

func badge(s string, noColor bool) string {
	if noColor {
		return "[" + s + "]"
	}
	return "\x1b[36m[" + s + "]\x1b[0m"
}

func faint(noColor bool) string {
	if noColor {
		return ""
	}
	return "\x1b[2m"
}

func reset(noColor bool) string {
	if noColor {
		return ""
	}
	return "\x1b[0m"
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
