package greet

import "fmt"

// Hello returns a short greeting for name.
func Hello(name string) string {
	return fmt.Sprintf("hello, %s", name)
}
