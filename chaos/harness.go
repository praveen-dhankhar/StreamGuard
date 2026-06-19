//go:build chaos_enabled

package chaos

import (
	"errors"
	"os"
)

func Enabled() error {
	if os.Getenv("STREAMGUARD_CHAOS_ENABLED") != "true" {
		return errors.New("chaos harness requires STREAMGUARD_CHAOS_ENABLED=true")
	}
	return nil
}
