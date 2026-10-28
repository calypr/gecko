package storagepath

import "strings"

var partReplacer = strings.NewReplacer("/", "_", "\\", "_", ":", "_", " ", "_")

func SanitizePart(value string) string {
	return partReplacer.Replace(value)
}
