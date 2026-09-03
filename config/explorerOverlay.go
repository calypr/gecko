package config

// ExplorerOverlay is the semantic, revisionable form of explorer customization.
// It intentionally stores references by semanticPath rather than physical SQL names.
type ExplorerOverlay struct {
	SchemaVersion  int                  `json:"schemaVersion"`
	ProjectID      string               `json:"projectId,omitempty"`
	SharedFilters  OverlaySharedFilters `json:"sharedFilters,omitempty"`
	ExplorerConfig []OverlayTab         `json:"explorerConfig"`
	FileActions    OverlayFileActions   `json:"fileActions,omitempty"`
	// LegacyConfig preserves the complete expanded v1 document during the
	// migration window. Its field references are validated exactly by physical
	// name; resolved reads return it unchanged until an authored v2 overlay is
	// published.
	LegacyConfig *Config `json:"legacyConfig,omitempty"`
}

type OverlayTab struct {
	TabID                     string                  `json:"tabId,omitempty"`
	DataType                  string                  `json:"dataType"`
	TabTitle                  string                  `json:"tabTitle,omitempty"`
	IncludeUnconfiguredFields *bool                   `json:"includeUnconfiguredFields,omitempty"`
	GuppyConfig               OverlayGuppyConfig      `json:"guppyConfig,omitempty"`
	Charts                    map[string]OverlayChart `json:"charts,omitempty"`
	Filters                   OverlayFiltersConfig    `json:"filters,omitempty"`
	Table                     *OverlayTableConfig     `json:"table,omitempty"`
	Dropdowns                 map[string]any          `json:"dropdowns,omitempty"`
	Buttons                   []OverlayButton         `json:"buttons,omitempty"`
	LoginForDownload          bool                    `json:"loginForDownload,omitempty"`
	PreFilters                map[string]any          `json:"preFilters,omitempty"`
	Fields                    []OverlayField          `json:"fields,omitempty"`
}

// OverlayGuppyConfig contains Guppy settings. Every value which names a
// dataframe column uses semanticPath; the resolver translates those names to
// the physical names expected by the existing Explorer frontend.
type OverlayGuppyConfig struct {
	NodeCountTitle            string                     `json:"nodeCountTitle,omitempty"`
	FieldMapping              []OverlayGuppyFieldMapping `json:"fieldMapping,omitempty"`
	AccessibleFieldCheckList  []string                   `json:"accessibleFieldCheckList,omitempty"`
	AccessibleValidationField string                     `json:"accessibleValidationField,omitempty"`
	ManifestMapping           OverlayManifestMapping     `json:"manifestMapping,omitempty"`
}
type OverlayGuppyFieldMapping struct {
	SemanticPath string `json:"semanticPath"`
	Name         string `json:"name,omitempty"`
}
type OverlayManifestMapping struct {
	ResourceIndexType                      string `json:"resourceIndexType,omitempty"`
	ResourceIDSemanticPath                 string `json:"resourceIdSemanticPath,omitempty"`
	ReferenceIDSemanticPathInResourceIndex string `json:"referenceIdSemanticPathInResourceIndex,omitempty"`
	ReferenceIDSemanticPathInDataIndex     string `json:"referenceIdSemanticPathInDataIndex,omitempty"`
}

type OverlayFiltersConfig struct {
	Tabs []OverlayFilterTab `json:"tabs,omitempty"`
}
type OverlayFilterTab struct {
	Title        string                        `json:"title,omitempty"`
	Fields       []OverlayFilterField          `json:"fields,omitempty"`
	FieldsConfig map[string]OverlayFieldConfig `json:"fieldsConfig,omitempty"`
}
type OverlayFilterField struct {
	SemanticPath string `json:"semanticPath"`
}
type OverlayFieldConfig struct {
	DataFieldSemanticPath string `json:"dataFieldSemanticPath,omitempty"`
	IndexSemanticPath     string `json:"indexSemanticPath,omitempty"`
	Label                 string `json:"label,omitempty"`
	Type                  string `json:"type,omitempty"`
}

type OverlayTableConfig struct {
	Enabled       *bool                         `json:"enabled,omitempty"`
	Fields        []string                      `json:"fields,omitempty"`
	Columns       map[string]OverlayTableColumn `json:"columns,omitempty"`
	DetailsConfig OverlayTableDetailsConfig     `json:"detailsConfig,omitempty"`
}
type OverlayTableColumn struct {
	Title              string         `json:"title,omitempty"`
	AccessorPath       string         `json:"accessorPath,omitempty"`
	Type               string         `json:"type,omitempty"`
	CellRenderFunction string         `json:"cellRenderFunction,omitempty"`
	Params             map[string]any `json:"params,omitempty"`
	Width              string         `json:"width,omitempty"`
	Sortable           bool           `json:"sortable,omitempty"`
	Visible            bool           `json:"visible,omitempty"`
}
type OverlayTableDetailsConfig struct {
	Panel              string            `json:"panel,omitempty"`
	Mode               string            `json:"mode,omitempty"`
	IDSemanticPath     string            `json:"idSemanticPath,omitempty"`
	FilterSemanticPath string            `json:"filterSemanticPath,omitempty"`
	Title              string            `json:"title,omitempty"`
	NodeType           string            `json:"nodeType,omitempty"`
	NodeFields         map[string]string `json:"nodeFields,omitempty"`
}

type OverlayButton struct {
	Enabled    bool                    `json:"enabled,omitempty"`
	Type       string                  `json:"type,omitempty"`
	Action     string                  `json:"action,omitempty"`
	Title      string                  `json:"title,omitempty"`
	LeftIcon   string                  `json:"leftIcon,omitempty"`
	RightIcon  string                  `json:"rightIcon,omitempty"`
	FileName   string                  `json:"fileName,omitempty"`
	ActionArgs OverlayButtonActionArgs `json:"actionArgs,omitempty"`
}
type OverlayButtonActionArgs struct {
	ResourceIndexType                      string   `json:"resourceIndexType,omitempty"`
	ResourceIDSemanticPath                 string   `json:"resourceIdSemanticPath,omitempty"`
	ReferenceIDSemanticPathInDataIndex     string   `json:"referenceIdSemanticPathInDataIndex,omitempty"`
	ReferenceIDSemanticPathInResourceIndex string   `json:"referenceIdSemanticPathInResourceIndex,omitempty"`
	FileSemanticPaths                      []string `json:"fileSemanticPaths,omitempty"`
}

type OverlaySharedFilters struct {
	Defined map[string][]OverlayFilterPair `json:"defined,omitempty"`
}
type OverlayFilterPair struct {
	Index        string `json:"index"`
	SemanticPath string `json:"semanticPath"`
}
type OverlayFileActions struct {
	Extensions map[string][]string `json:"extensions,omitempty"`
	Actions    map[string]string   `json:"actions,omitempty"`
}

type OverlayField struct {
	SemanticPath   string           `json:"semanticPath"`
	MissingPolicy  string           `json:"missingPolicy,omitempty"`
	OmissionReason string           `json:"omissionReason,omitempty"`
	Chart          *OverlayChart    `json:"chart,omitempty"`
	Filters        []OverlayFilter  `json:"filters,omitempty"`
	Table          *OverlayTable    `json:"table,omitempty"`
	Download       *OverlayDownload `json:"download,omitempty"`
	Renderer       string           `json:"renderer,omitempty"`
	Params         map[string]any   `json:"params,omitempty"`
}

type OverlayChart struct {
	ChartType string `json:"chartType"`
	Title     string `json:"title,omitempty"`
}
type OverlayFilter struct {
	Type  string `json:"type,omitempty"`
	Label string `json:"label,omitempty"`
}
type OverlayTable struct {
	Include  *bool  `json:"include,omitempty"`
	Title    string `json:"title,omitempty"`
	Sortable *bool  `json:"sortable,omitempty"`
}
type OverlayDownload struct {
	Include *bool `json:"include,omitempty"`
}

func (o ExplorerOverlay) Version() int {
	if o.SchemaVersion == 0 {
		return 1
	}
	return o.SchemaVersion
}
