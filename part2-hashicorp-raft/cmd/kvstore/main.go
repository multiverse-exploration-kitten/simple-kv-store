// Command kvstore runs one node of the replicated key-value store.
//
//	kvstore [serve]       run the node (configuration from KV_* variables)
//	kvstore healthcheck   exit 0 when the local node answers /health/live
//	kvstore gencerts      create the Raft TLS CA and node certificates
//
// The container image has no shell, so the healthcheck is a subcommand.
package main

import (
	"fmt"
	"os"
)

const usage = `usage: kvstore [serve | healthcheck | gencerts [-dir DIR] [-peers id=host:port,...] [-export-node ID -export-dir DIR]]`

func main() {
	subcommand, arguments := "serve", []string(nil)
	if len(os.Args) > 1 {
		subcommand, arguments = os.Args[1], os.Args[2:]
	}
	var err error
	switch subcommand {
	case "serve":
		err = runServe()
	case "healthcheck":
		err = runHealthcheck()
	case "gencerts":
		err = runGencerts(arguments)
	default:
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "kvstore %s: %v\n", subcommand, err)
		os.Exit(1)
	}
}
