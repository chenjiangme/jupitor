package main

import (
	"flag"
	"fmt"
	"os"
)

const version = "0.1.0"

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: jupitor-cli <command> [options]\n\n")
		fmt.Fprintf(os.Stderr, "Commands:\n")
		fmt.Fprintf(os.Stderr, "  version    Print the CLI version\n")
		fmt.Fprintf(os.Stderr, "  status     Show jupitor-server status\n")
		fmt.Fprintf(os.Stderr, "  symbols    List available symbols\n")
		fmt.Fprintf(os.Stderr, "\n")
	}

	if len(os.Args) < 2 {
		flag.Usage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "version":
		fmt.Printf("jupitor-cli %s\n", version)

	case "status":
		// TODO: Connect to jupitor-server API and retrieve status.
		fmt.Println("status: not implemented")

	case "symbols":
		// TODO: Connect to jupitor-server API and list symbols.
		fmt.Println("symbols: not implemented")

	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n\n", os.Args[1])
		flag.Usage()
		os.Exit(1)
	}
}
