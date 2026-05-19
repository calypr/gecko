package config

type FilesummaryConfig struct {
	Config         map[string]TableColumnsConfig `json:"config"`
	BarChartColor  string                        `json:"barChartColor"`
	DefaultProject string                        `json:"defaultProject"`
	BinslicePoints []int                         `json:"binslicePoints"`
	IdField        string                        `json:"idField"`
	Index          string                        `json:"index"`
}
