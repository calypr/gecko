package presentation

import "errors"

const (
	ProjectPresentationDirectory = "_project_presentations"
)

var (
	ErrNoPresentation  = errors.New("project has no presentation HTML")
	ErrDataDirRequired = errors.New("presentation storage directory is required")
)
