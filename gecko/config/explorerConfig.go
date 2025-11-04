package config

import "encoding/json"

type Reports struct {
	Title       string      `json:"Title"`
	TableConfig TableConfig `json:"tableConfig"`
}

type FieldConfig struct {
	Field     string `json:"field,omitempty"`
	DataField string `json:"dataField,omitempty"`
	Index     string `json:"index,omitempty"`
	Label     string `json:"label"`
	Type      string `json:"type,omitempty"`
}

type FilterTab struct {
	Title        string                 `json:"title,omitempty"`
	Fields       []string               `json:"fields"`
	FieldsConfig map[string]FieldConfig `json:"fieldsConfig,omitempty"`
}

type FiltersConfig struct {
	Tabs []FilterTab `json:"tabs"`
}

type TableConfig struct {
	Enabled       bool                          `json:"enabled"`
	Fields        []string                      `json:"fields"`
	Columns       map[string]TableColumnsConfig `json:"columns,omitempty"`
	DetailsConfig TableDetailsConfig            `json:"detailsConfig"`
}

type SummaryTableColumnType string

// Constants define the allowed values for SummaryTableColumnType.
const (
	// SummaryTableColumnTypeString represents a string column type.
	SummaryTableColumnTypeString SummaryTableColumnType = "string"
	// SummaryTableColumnTypeNumber represents a number column type.
	SummaryTableColumnTypeNumber SummaryTableColumnType = "number"
	// SummaryTableColumnTypeDate represents a date column type.
	SummaryTableColumnTypeDate SummaryTableColumnType = "date"
	// SummaryTableColumnTypeArray represents an array column type.
	SummaryTableColumnTypeArray SummaryTableColumnType = "array"
	// SummaryTableColumnTypeLink represents a link column type.
	SummaryTableColumnTypeLink SummaryTableColumnType = "link"
	// SummaryTableColumnTypeBoolean represents a boolean column type.
	SummaryTableColumnTypeBoolean SummaryTableColumnType = "boolean"
	// SummaryTableColumnTypeParagraphs represents a paragraphs column type.
	SummaryTableColumnTypeParagraphs SummaryTableColumnType = "paragraphs"
)

type TableColumnsConfig struct {
	Field              string                 `json:"field"`
	Title              string                 `json:"title"`
	AccessorPath       string                 `json:"accessorPath,omitempty"`
	Type               SummaryTableColumnType `json:"type,omitempty"`
	CellRenderFunction string                 `json:"cellRenderFunction,omitempty"`
	Params             map[string]any         `json:"params,omitempty"`
	Width              int                    `json:"width,omitempty"`
	Sortable           bool                   `json:"sortable,omitempty"`
	Visable            bool                   `json:"visable,omitempty"`
}

// MarshalJSON omits "type" field when it's zero value
func (t TableColumnsConfig) MarshalJSON() ([]byte, error) {
	type Alias TableColumnsConfig
	aux := struct {
		*Alias
		Type *SummaryTableColumnType `json:"type,omitempty"` // pointer → omits if nil
	}{
		Alias: (*Alias)(&t),
	}
	if t.Type != "" {
		aux.Type = &t.Type
	}
	return json.Marshal(aux)
}

type TableDetailsConfig struct {
	Panel       string            `json:"panel,omitempty"`
	Mode        string            `json:"mode,omitempty"`
	IDField     string            `json:"idField,omitempty"`
	FilterField string            `json:"filterField,omitempty"`
	Title       string            `json:"title,omitempty"`
	NodeType    string            `json:"nodeType,omitempty"`
	NodeFields  map[string]string `json:"nodeFields,omitempty"`
}

type GuppyConfig struct {
	DataType                  string              `json:"dataType"`
	NodeCountTitle            string              `json:"nodeCountTitle"`
	FieldMapping              []GuppyFieldMapping `json:"fieldMapping,omitempty"`
	AccessibleFieldCheckList  []string            `json:"accessibleFieldCheckList,omitempty"`
	AccessibleValidationField string              `json:"accessibleValidationField,omitempty"`
	ManifestMapping           ManifestMapping     `json:"manifestMapping"`
}

type GuppyFieldMapping struct {
	Field string `json:"field,omitempty"`
	Name  string `json:"name,omitempty"`
}

type ManifestMapping struct {
	ResourceIndexType               string `json:"resourceIndexType,omitempty"`
	ResourceIdField                 string `json:"resourceIdField,omitempty"`
	ReferenceIdFieldInResourceIndex string `json:"referenceIdFieldInResourceIndex,omitempty"`
	ReferenceIdFieldInDataIndex     string `json:"referenceIdFieldInDataIndex,omitempty"`
}

type Chart struct {
	ChartType string `json:"chartType"`
	Title     string `json:"title"`
}

type ButtonConfig struct {
	Enabled    bool             `json:"enabled,omitempty"`
	Type       string           `json:"type,omitempty"`
	Action     string           `json:"action,omitempty"`
	Title      string           `json:"title,omitempty"`
	LeftIcon   string           `json:"leftIcon,omitempty"`
	RightIcon  string           `json:"rightIcon,omitempty"`
	FileName   string           `json:"fileName,omitempty"`
	ActionArgs ButtonActionArgs `json:"actionArgs"`
}

type ButtonActionArgs struct {
	ResourceIndexType               string   `json:"resourceIndexType,omitempty"`
	ResourceIdField                 string   `json:"resourceIdField,omitempty"`
	ReferenceIdFieldInDataIndex     string   `json:"referenceIdFieldInDataIndex,omitempty"`
	ReferenceIdFieldInResourceIndex string   `json:"referenceIdFieldInResourceIndex,omitempty"`
	FileFields                      []string `json:"fileFields,omitempty"`
}

// ConfigItem represents an individual configuration item
// @Schema
type ConfigItem struct {
	TabTitle         string           `json:"tabTitle"`
	GuppyConfig      GuppyConfig      `json:"guppyConfig"`
	Charts           map[string]Chart `json:"charts,omitempty"`
	Filters          FiltersConfig    `json:"filters"`
	Table            TableConfig      `json:"table"`
	Dropdowns        map[string]any   `json:"dropdowns,omitempty"`
	Buttons          []ButtonConfig   `json:"buttons,omitempty"`
	LoginForDownload bool             `json:"loginForDownload,omitempty"`
}

type Config struct {
	SharedFilters  SharedFiltersConfig `json:"sharedFilters"`
	ExplorerConfig []ConfigItem        `json:"explorerConfig"`
}

type SharedFiltersConfig struct {
	SharedFilter map[string][]FilterPair `json:"defined"`
}

type FilterPair struct {
	Index string `json:"index"`
	Field string `json:"field"`
}
