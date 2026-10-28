package config

import "fmt"

type PresentationConfig struct {
	PresentationConfig string `json:"presentationConfig"`
}

func (p *PresentationConfig) Validate() error {
	if p == nil {
		return fmt.Errorf("presentation config is required")
	}
	return nil
}
