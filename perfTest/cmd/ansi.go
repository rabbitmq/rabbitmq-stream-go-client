package cmd

import "os"

// ANSI styling without external deps. Respects https://no-color.org/ and TERM=dumb.
func ansiEnabled() bool {
	if noColor {
		return false
	}
	if os.Getenv("NO_COLOR") != "" {
		return false
	}
	if os.Getenv("FORCE_COLOR") != "" {
		return true
	}
	t := os.Getenv("TERM")
	return t != "" && t != "dumb"
}

const esc = "\033["

// Reset and common SGR codes (ECMA-48).
const (
	ansiReset   = esc + "0m"
	ansiBold    = esc + "1m"
	ansiDim     = esc + "2m"
	ansiRed     = esc + "31m"
	ansiGreen   = esc + "32m"
	ansiYellow  = esc + "33m"
	ansiBlue    = esc + "34m"
	ansiMagenta = esc + "35m"
	ansiCyan    = esc + "36m"
	ansiWhite   = esc + "37m"
	ansiGray    = esc + "90m"
	ansiHiGreen = esc + "92m"
	ansiHiCyan  = esc + "96m"
)

// style wraps text with SGR; no-op when colors disabled.
func style(seq, text string) string {
	if !ansiEnabled() || text == "" {
		return text
	}
	return seq + text + ansiReset
}

// opTag is a short colored label for a class of work (streams, publish, consume, etc.).
func opTag(label, seq string) string {
	if !ansiEnabled() {
		return "[" + label + "]"
	}
	return style(seq+ansiBold, "["+label+"]")
}
