package utils

import "strings"

func EscapePath(path string) string {
	// escape %
	escapedPath := strings.ReplaceAll(path, "%", "%25")

	// add others

	return escapedPath
}

func UnescapePath(path string) string {
	// unescape %
	unescapedPath := strings.ReplaceAll(path, "%25", "%")

	// add others

	return unescapedPath
}
