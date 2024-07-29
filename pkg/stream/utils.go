package stream

import (
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type responseError struct {
	Err       error
	isTimeout bool
}

func newResponseError(err error, timeout bool) responseError {
	return responseError{
		Err:       err,
		isTimeout: timeout,
	}
}

func uShortExtractResponseCode(code uint16) uint16 {
	return code & 0b0111_1111_1111_1111
}

//func UIntExtractResponseCode(code int32) int32 {
//	return code & 0b0111_1111_1111_1111
//}

func uShortEncodeResponseCode(code uint16) uint16 {
	return code | 0b1000_0000_0000_0000
}

func waitCodeWithDefaultTimeOut(response *Response) responseError {
	return waitCodeWithTimeOut(response, defaultSocketCallTimeout)
}
func waitCodeWithTimeOut(response *Response, timeout time.Duration) responseError {
	select {
	case code := <-response.code:
		if code.id != responseCodeOk {
			return newResponseError(lookErrorCode(code.id), false)
		}
		return newResponseError(nil, false)
	case <-time.After(timeout):
		logs.LogError("timeout %d ns - waiting Code, operation: %s", timeout.Milliseconds(), response.commandDescription)

		return newResponseError(
			fmt.Errorf("timeout %d ms - waiting Code, operation: %s ",
				timeout.Milliseconds(), response.commandDescription), true)
	}
}

func SetLevelInfo(value int8) {
	logs.LogLevel = value
}

func containsOnlySpaces(input string) bool {
	return len(input) > 0 && len(strings.TrimSpace(input)) == 0
}

//private static string ExtractVersion(string fullVersion)
//{
//const string Pattern = @"(\d+\.\d+\.\d+)";
//var match = Regex.Match(fullVersion, Pattern);
//
//return match.Success
//? match.Groups[1].Value
//: string.Empty;
//}

func ExtractVersion(fullVersion string) string {
	const pattern = `(\d+\.\d+\.\d+)`
	re := regexp.MustCompile(pattern)
	match := re.FindStringSubmatch(fullVersion)
	if len(match) > 0 {
		return match[1]
	}
	return ""
}

func Is311OrMore(version string) (bool, error) {
	v := ExtractVersion(version)
	compare, err := CompareSemver(v, "3.11.0")
	if err != nil {
		return false, err
	}
	return compare >= 0, nil
}

// CompareSemver compares two semantic version strings.
// Returns -1 if v1 < v2, 1 if v1 > v2, and 0 if they are equal.
func CompareSemver(v1, v2 string) (int, error) {
	parseVersion := func(v string) ([]int, error) {
		parts := strings.Split(v, ".")
		if len(parts) != 3 {
			return nil, fmt.Errorf("invalid semantic version: %s", v)
		}
		version := make([]int, 3)
		for i, part := range parts {
			num, err := strconv.Atoi(part)
			if err != nil {
				return nil, fmt.Errorf("invalid number in version: %s", part)
			}
			version[i] = num
		}
		return version, nil
	}

	v1Parts, err := parseVersion(v1)
	if err != nil {
		return 0, err
	}
	v2Parts, err := parseVersion(v2)
	if err != nil {
		return 0, err
	}

	for i := 0; i < 3; i++ {
		if v1Parts[i] < v2Parts[i] {
			return -1, nil
		} else if v1Parts[i] > v2Parts[i] {
			return 1, nil
		}
	}
	return 0, nil
}
