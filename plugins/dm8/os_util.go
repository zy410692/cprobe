package dm8

import (
	"runtime"
	"strings"
)

const (
	OS_LINUX = "Linux"

	OS_WINDOWS = "Windows"

	OS_UNKNOWN = "Unknown"
)

func GetOS() string {
	goos := runtime.GOOS
	switch strings.ToLower(goos) {
	case "windows":
		return OS_WINDOWS
	case "linux":
		return OS_LINUX
	default:
		return OS_UNKNOWN
	}
}
