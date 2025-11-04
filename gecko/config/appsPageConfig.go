package config

type AppCard struct {
	Title       string `json:"title"`
	Description string `json:"description"`
	Icon        string `json:"icon"`
	Href        string `json:"href"`
	Perms       string `json:"perms"`
}

type AppsConfig struct {
	AppCards []AppCard `json:"appCards"`
}
