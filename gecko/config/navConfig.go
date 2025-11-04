package config

import (
	"encoding/json"
)

// ---------------------------------------------------------------------
// NavigationButtonProps
// ---------------------------------------------------------------------
type NavigationButtonProps struct {
	Icon       string                           `json:"icon"`
	Tooltip    string                           `json:"tooltip"`
	Href       string                           `json:"href"`
	NoBasePath *bool                            `json:"noBasePath,omitempty"`
	Name       string                           `json:"name"`
	IconHeight string                           `json:"iconHeight,omitempty"`
	Title      string                           `json:"title,omitempty"` // present in the example
	ClassNames *StylingOverrideWithMergeControl `json:"classNames,omitempty"`
}

// ---------------------------------------------------------------------
// NavigationBarLogo
// ---------------------------------------------------------------------
type NavigationBarLogo struct {
	Src         string                           `json:"src"`
	Title       string                           `json:"title,omitempty"`
	Description string                           `json:"description,omitempty"`
	Width       float64                          `json:"width,omitempty"` // JSON numbers → float64
	Height      float64                          `json:"height,omitempty"`
	NoBasePath  *bool                            `json:"noBasePath,omitempty"`
	Divider     *bool                            `json:"divider,omitempty"`
	BasePath    string                           `json:"basePath,omitempty"`
	Href        string                           `json:"href"`
	OnToggle    json.RawMessage                  `json:"onToggle,omitempty"` // function → ignored on server side
	Basepage    *bool                            `json:"basepage,omitempty"`
	ClassNames  *StylingOverrideWithMergeControl `json:"classNames,omitempty"`
}

// ---------------------------------------------------------------------
// NavigationProps
// ---------------------------------------------------------------------
type NavigationProps struct {
	Logo       *NavigationBarLogo               `json:"logo,omitempty"`
	Items      []NavigationButtonProps          `json:"items"`
	Title      string                           `json:"title,omitempty"`
	LoginIcon  json.RawMessage                  `json:"loginIcon,omitempty"` // ReactElement | string
	ClassNames *StylingOverrideWithMergeControl `json:"classNames"`
}

// ---------------------------------------------------------------------
// LeftNavBarProps
// ---------------------------------------------------------------------
type LeftNavBarProps struct {
	Title       string `json:"title"`
	Description string `json:"description"`
	Icon        string `json:"icon"`
	Href        string `json:"href"`
	Perms       any    `json:"perms"` // can be string | null
}

// ---------------------------------------------------------------------
// TopBarItem (inside TopBarProps)
// ---------------------------------------------------------------------
type TopBarItem struct {
	ClassNames struct {
		Button string `json:"button"`
		Label  string `json:"label"`
		Root   string `json:"root"`
	} `json:"classNames,omitempty"`
	Href string `json:"href,omitempty"`
	Name string `json:"name,omitempty"`
}

// ---------------------------------------------------------------------
// TopBarProps
// ---------------------------------------------------------------------
type TopBarProps struct {
	Items                 []TopBarItem `json:"items,omitempty"`
	LoginButtonVisibility string       `json:"loginButtonVisibility,omitempty"` // "hidden" | …
}

// ---------------------------------------------------------------------
// HeaderProps (only the fields that appear in the config)
// ---------------------------------------------------------------------
type HeaderProps struct {
	Top        TopBarProps       `json:"topBar"` // name in JSON is "topBar"
	Navigation NavigationProps   `json:"navigation"`
	LeftNav    []LeftNavBarProps `json:"leftnav"`
	BasePage   bool              `json:"basePage,omitempty"`
	// siteProps, banners, type, children … are omitted because they are not in the example
}

// ---------------------------------------------------------------------
// NavPageLayoutProps – the full layout object
// ---------------------------------------------------------------------
type NavPageLayoutProps struct {
	HeaderProps    HeaderProps    `json:"headerProps"` // we only have the part that is in the example
	FooterProps    FooterProps    `json:"footerProps"` // re-use existing FooterProps
	HeaderMetadata HeaderMetadata `json:"headerMetadata"`
	// MainProps, CustomHeaderComponent, CustomFooterComponent omitted (not in example)
}

// ---------------------------------------------------------------------
// HeaderMetadata
// ---------------------------------------------------------------------
type HeaderMetadata struct {
	Title   string `json:"title"`
	Content string `json:"content"`
	Key     string `json:"key"`
}
