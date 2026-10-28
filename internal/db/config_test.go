package db

import "testing"

func TestValidateConfigType(t *testing.T) {
	if err := validateConfigType("projects"); err != nil {
		t.Fatalf("expected known config type to pass: %v", err)
	}
	if err := validateConfigType("not_a_config_type"); err == nil {
		t.Fatal("expected unknown config type to fail")
	}
}
