// SPDY client-go oracle: drives the real k8s.io/client-go remotecommand
// SPDYExecutor against a URL (arg 1). This is the SAME client code path
// critest uses (NewSPDYExecutor), isolated from the CRI container lifecycle.
//
// Exit 0 + "ORACLE_OK" on success; non-zero + "ORACLE_FAIL: <err>" on the
// client-go failure (the same "failed to open streamer" critest reports).
package main

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"
	"time"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("ORACLE_FAIL: usage: oracle <url>")
		os.Exit(2)
	}
	rawURL := os.Args[1]
	u, err := url.Parse(rawURL)
	if err != nil {
		fmt.Printf("ORACLE_FAIL: parse url: %v\n", err)
		os.Exit(2)
	}

	cfg := &rest.Config{Host: u.Host}
	exec, err := remotecommand.NewSPDYExecutor(cfg, "POST", u)
	if err != nil {
		fmt.Printf("ORACLE_FAIL: NewSPDYExecutor: %v\n", err)
		os.Exit(1)
	}

	var stdout, stderr bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
		Tty:    false,
	})
	if err != nil {
		fmt.Printf("ORACLE_FAIL: StreamWithContext: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("ORACLE_OK stdout=%q stderr=%q\n", stdout.String(), stderr.String())
}
