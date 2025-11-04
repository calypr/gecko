package config

import (
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------
// Styling
// ---------------------------------------------------------------------
type StylingMergeMode string

const (
	MergeModeReplace StylingMergeMode = "replace"
	MergeModeMerge   StylingMergeMode = "merge"
)

type StylingOverrideWithMergeControl struct {
	Root            string           `json:"root"`
	Layout          string           `json:"layout,omitempty"`
	MergeMode       StylingMergeMode `json:"mergeMode,omitempty"`
	NavigationPanel string           `json:"navigationPanel,omitempty"`
	Button          string           `json:"button,omitempty"`
	Label           string           `json:"label,omitempty"`
	Item            string           `json:"item,omitempty"`
}

// ---------------------------------------------------------------------
// Core Types
// ---------------------------------------------------------------------
type LinkType string

const (
	LinkTypeGen3FF LinkType = "gen3ff"
	LinkTypePortal LinkType = "portal"
)

type BottomLinks struct {
	Text string `json:"text"`
	Href string `json:"href"`
}

type ColumnLinks struct {
	Heading string `json:"heading"`
	Items   []struct {
		Text     string   `json:"text"`
		Href     string   `json:"href,omitempty"`
		LinkType LinkType `json:"linkType,omitempty"`
	} `json:"items"`
}

type FooterText struct {
	Text      string `json:"text"`
	ClassName string `json:"className,omitempty"`
}

type FooterLink struct {
	FooterText
	Href     string   `json:"href"`
	LinkType LinkType `json:"linkType,omitempty"`
}

type FooterLinks struct {
	Links     []FooterLink `json:"links"`
	ClassName string       `json:"className,omitempty"`
}

type FooterLogo struct {
	LogoLight   string `json:"logolight"`
	Logo        string `json:"logo"`
	Description string `json:"description"`
	Width       int    `json:"width"`
	Height      int    `json:"height"`
	ClassName   string `json:"className,omitempty"`
	Href        string `json:"href,omitempty"`
}

// ---------------------------------------------------------------------
// FooterRow – Now a struct with custom JSON (un)marshaling
// ---------------------------------------------------------------------

type FooterRow struct {
	Kind    string
	Icon    *FooterLogo
	Text    *FooterText
	Link    *FooterLink
	Links   *FooterLinks
	Section *FooterSectionProps
}

func (r *FooterRow) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	if len(raw) != 1 {
		return fmt.Errorf("FooterRow must have exactly one key, got %d", len(raw))
	}

	for key, value := range raw {
		r.Kind = key
		switch key {
		case "Icon":
			var logo FooterLogo
			if err := json.Unmarshal(value, &logo); err != nil {
				return err
			}
			r.Icon = &logo

		case "Text":
			var text FooterText
			if err := json.Unmarshal(value, &text); err != nil {
				return err
			}
			r.Text = &text

		case "Link":
			var link FooterLink
			if err := json.Unmarshal(value, &link); err != nil {
				return err
			}
			r.Link = &link

		case "Links":
			var links FooterLinks
			if err := json.Unmarshal(value, &links); err != nil {
				return err
			}
			r.Links = &links

		case "Section":
			var section FooterSectionProps
			if err := json.Unmarshal(value, &section); err != nil {
				return err
			}
			r.Section = &section

		default:
			return fmt.Errorf("unknown FooterRow key: %s", key)
		}
	}
	return nil
}

// MarshalJSON for round-trip
func (r FooterRow) MarshalJSON() ([]byte, error) {
	var obj map[string]any
	switch r.Kind {
	case "Icon":
		if r.Icon == nil {
			return nil, fmt.Errorf("Logo is nil")
		}
		obj = map[string]any{"Icon": r.Icon}
	case "Text":
		if r.Text == nil {
			return nil, fmt.Errorf("Text is nil")
		}
		obj = map[string]any{"Text": r.Text}
	case "Link":
		if r.Link == nil {
			return nil, fmt.Errorf("Link is nil")
		}
		obj = map[string]any{"Link": r.Link}
	case "Links":
		if r.Links == nil {
			return nil, fmt.Errorf("Links is nil")
		}
		obj = map[string]any{"Links": r.Links}
	case "Section":
		if r.Section == nil {
			return nil, fmt.Errorf("Section is nil")
		}
		obj = map[string]any{"Section": r.Section}
	default:
		return nil, fmt.Errorf("unknown FooterRow kind: %s", r.Kind)
	}
	return json.Marshal(obj)
}

// ---------------------------------------------------------------------
// Column / Section
// ---------------------------------------------------------------------
type FooterColumnProps struct {
	Heading    string                           `json:"heading,omitempty"`
	Rows       []FooterRow                      `json:"rows"`
	ClassNames *StylingOverrideWithMergeControl `json:"classNames,omitempty"`
	BasePage   bool                             `json:"basePage,omitempty"`
}

type FooterSectionProps struct {
	Columns   []FooterColumnProps `json:"columns"`
	ClassName string              `json:"className,omitempty"`
	BasePage  bool                `json:"basePage,omitempty"`
}

// ---------------------------------------------------------------------
// FooterProps
// ---------------------------------------------------------------------
type FooterProps struct {
	BottomLinks      []BottomLinks                    `json:"bottomLinks,omitempty"`
	ColumnLinks      []ColumnLinks                    `json:"columnLinks,omitempty"`
	FooterLogos      []FooterLogo                     `json:"footerLogos,omitempty"`
	FooterRightLogos []FooterLogo                     `json:"footerRightLogos,omitempty"`
	RightSection     *FooterSectionProps              `json:"rightSection,omitempty"`
	LeftSection      *FooterSectionProps              `json:"leftSection,omitempty"`
	ClassNames       *StylingOverrideWithMergeControl `json:"classNames,omitempty"`
	CustomFooter     json.RawMessage                  `json:"customFooter,omitempty"`
	BasePage         bool                             `json:"basePage,omitempty"`
}
