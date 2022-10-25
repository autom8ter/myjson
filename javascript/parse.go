package javascript

import (
	"bufio"
	"strings"
)

func getFunctionName(script string) string {
	scanner := bufio.NewScanner(strings.NewReader(script))
	scanner.Split(bufio.ScanWords)
	isNext := false
	for scanner.Scan() {
		word := scanner.Text()
		if word == "function" {
			isNext = true
			continue
		}
		if isNext {
			before, _, found := strings.Cut(word, "(")
			if found {
				return strings.TrimSpace(before)
			}
			isNext = false
		}
	}
	return ""
}
