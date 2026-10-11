package config

import "testing"

func TestNavPageLayoutPropsIsZero(t *testing.T) {
	tests := []struct {
		name string
		cfg  NavPageLayoutProps
		want bool
	}{
		{
			name: "all sections empty",
			cfg:  NavPageLayoutProps{},
			want: true,
		},
		{
			name: "nav without footer right section",
			cfg: NavPageLayoutProps{
				HeaderProps: HeaderProps{
					LeftNav: []LeftNavBarProps{{Title: "Overview"}},
				},
				HeaderMetadata: HeaderMetadata{Title: "Genome Browser"},
			},
			want: false,
		},
		{
			name: "footer with right section columns only",
			cfg: NavPageLayoutProps{
				FooterProps: FooterProps{
					RightSection: &FooterSectionProps{
						Columns: []FooterColumnProps{{Heading: "Resources"}},
					},
				},
			},
			want: false,
		},
		{
			name: "footer with bottom links only",
			cfg: NavPageLayoutProps{
				FooterProps: FooterProps{
					BottomLinks: []BottomLinks{{Text: "Privacy", Href: "/privacy"}},
				},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.cfg.IsZero(); got != tt.want {
				t.Errorf("NavPageLayoutProps.IsZero() = %v, want %v", got, tt.want)
			}
		})
	}
}
