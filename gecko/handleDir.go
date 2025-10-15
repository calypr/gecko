package gecko

import (
	"fmt"
	"net/http"
	"path"
	"strings"

	"github.com/bmeg/grip-graphql/middleware"
	"github.com/bmeg/grip/gripql"
	"github.com/kataras/iris/v12"
)

// handleListProjects godoc
// @Summary Retrieve directory information for a project
// @Description Retrieve directory details for the given project ID and Directory path
// @Tags Directory
// @Accept json
// @Produce json
// @Param project_id path string true "Project ID (format: program-project)"
// @Param body body DirectoryRequest true "Directory path"
// @Success 200 {object} map[string]interface{} "Directory information"
// @Failure 400 {object} ErrorResponse "Invalid request body or Directory path"
// @Failure 500 {object} ErrorResponse "Server error"
// @Router /dir/{project_id} [get]
func (server *Server) handleListProjects(ctx iris.Context) {
	projs, errResponse := server.GetProjectsFromToken(ctx, &middleware.ProdJWTHandler{})
	if errResponse != nil {
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		ctx.StopExecution()
		return
	}
	q := gripql.V().HasLabel("ResearchStudy").Has(gripql.Within("auth_resource_path", projs)).As("f0").Render(map[string]any{"project": "$f0.auth_resource_path"})
	res, err := server.gripqlClient.Traversal(
		ctx,
		&gripql.GraphQuery{Graph: server.gripGraphName, Query: q.Statements},
	)
	if err != nil {
		errResponse = newErrorResponse("internal server error", http.StatusInternalServerError, &err)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		ctx.StopExecution()
		return
	}
	out := []string{}
	for r := range res {
		renda, ok := r.GetRender().GetStructValue().AsMap()["project"].(string)
		if !ok {
			continue
		}
		out = append(out, renda)
	}
	jsonResponseFrom(out, 200)
}

type DirectoryRequest struct {
	Directory string `json:"Dir"` // POSIX path of the directory
}

type DirectoryResponse struct {
	Directories []map[string]any `json:"Directories"`
	Documents   []map[string]any `json:"Documents"`
	Message     string           `json:"Message"`
	Code        string           `json:"Code"`
}

// handleDirGet godoc
// @Summary Retrieve directory information for a project
// @Description Retrieve directory details for the given project ID and Directory path
// @Tags Directory
// @Accept json
// @Produce json
// @Param project_id path string true "Project ID (format: program-project)"
// @Param body body DirectoryRequest true "Directory path"
// @Success 200 {object} map[string]interface{} "Directory information"
// @Failure 400 {object} ErrorResponse "Invalid request body or Directory path"
// @Failure 500 {object} ErrorResponse "Server error"
// @Router /dir/{project_id} [get]
func (server *Server) handleDirGet(ctx iris.Context) {
	projectId := ctx.Params().Get("project_id")
	var req DirectoryRequest
	if err := ctx.ReadJSON(&req); err != nil {
		errResponse := newErrorResponse(fmt.Sprintf("Failed to parse request body: %v", err), http.StatusBadRequest, nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}

	if req.Directory == "" || !isValidPosixPath(&req.Directory) {
		errResponse := newErrorResponse(fmt.Sprintf("Invalid or missing Directory path"), http.StatusBadRequest, nil)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		return
	}

	q := gripql.V().HasLabel("ResearchStudy").Has(gripql.Eq("auth_resource_path", projectId)).
		OutE("rootDir_Directory").OutNull().HasLabel("Directory", "DocumentReference")

	q = buildQueryFromPath(&req.Directory, q)
	res, err := server.gripqlClient.Traversal(ctx, &gripql.GraphQuery{Graph: server.gripGraphName, Query: q.Statements})
	if err != nil {
		errResponse := newErrorResponse("internal server error", http.StatusInternalServerError, &err)
		errResponse.log.write(server.logger)
		_ = errResponse.write(ctx)
		ctx.StopExecution()
		return
	}
	out := []any{}
	for r := range res {
		out = append(out, r.GetVertex())
	}

	//ctx.JSON()
}

func buildQueryFromPath(p *string, query *gripql.Query) *gripql.Query {
	for splStr := range strings.SplitSeq(*p, "/") {
		query = query.OutNull().HasLabel("Directory", "DocumentReference").Has(gripql.Eq("name", splStr))
	}
	return query
}

func isValidPosixPath(p *string) bool {
	if strings.ContainsRune(*p, '\000') {
		return false
	}
	if path.IsAbs(*p) {
		return false
	}
	cleaned := path.Clean(*p)
	if *p == "" || cleaned == "." {
		return false
	}
	if strings.HasPrefix(cleaned, "..") {
		return false
	}
	if strings.Contains(*p, "\\") {
		return false
	}
	return true
}
