package config

type Configurable interface {
	IsZero() bool
}

// Update config.Config to implement the interface
func (c Config) IsZero() bool {
	return len(c.ExplorerConfig) == 0
}

// Implement the interface for config.FooterProps
func (fp FooterProps) IsZero() bool {
	return len(fp.BottomLinks) == 0 && len(fp.ColumnLinks) == 0 && fp.RightSection == nil
}

func (ap AppsConfig) IsZero() bool {
	return len(ap.AppCards) == 0
}

func (n NavPageLayoutProps) IsZero() bool {
	return len(n.HeaderProps.LeftNav) == 0 && len(n.FooterProps.RightSection.Columns) == 0
}

func (fs FilesummaryConfig) IsZero() bool {
	return len(fs.Config) == 0
}
