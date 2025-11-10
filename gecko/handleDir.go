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
// @Produce json
// @Param projectId path string true "Project ID (format: program-project)"
// @Success 200 {object} map[string]interface{} "Directory information"
// @Failure 400 {object} ErrorResponse "Invalid request body or Directory path"
// @Failure 500 {object} ErrorResponse "Server error"
// @Router /dir/{projectId} [get]
func (server *Server) handleListProjects(ctx iris.Context) {
	projs, errResponse := server.GetProjectsFromToken(ctx, &middleware.ProdJWTHandler{}, "read", "*")
	if errResponse != nil {
		errResponse.log.write(server.Logger)
		_ = errResponse.write(ctx)
		ctx.StopExecution()
		return
	}
	q := gripql.V().HasLabel("ResearchStudy").Has(gripql.Within("auth_resource_path", projs...)).As("f0").Render(map[string]any{"project": "$f0.auth_resource_path"})
	res, err := server.gripqlClient.Traversal(
		ctx,
		&gripql.GraphQuery{Graph: server.gripGraphName, Query: q.Statements},
	)
	if err != nil {
		errResponse = newErrorResponse("internal server error", http.StatusInternalServerError, &err)
		errResponse.log.write(server.Logger)
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
	jsonResponseFrom(out, 200).write(ctx)
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
// @Produce json
// @Param projectId path string true "Project ID (format: program-project)"
// @Param directory_path path string true "Directory Path (format: post path string)"
// @Success 200 {object} map[string]interface{} "Directory information"
// @Failure 400 {object} ErrorResponse "Invalid request body or Directory path"
// @Failure 403 {object} ErrorResponse "User is not allowed on any resource path"
// @Failure 500 {object} ErrorResponse "Server error"
// @Router /dir/{projectId}?directory={directory_path} [get]
func (server *Server) handleDirGet(ctx iris.Context) {
	projectId := ctx.Params().Get("projectId")
	dirPath := ctx.URLParam("directory")

	if dirPath == "" || !isValidPosixPath(&dirPath) {
		errResponse := newErrorResponse(fmt.Sprintf("Invalid or missing Directory path: '%s'", dirPath), http.StatusBadRequest, nil)
		errResponse.log.write(server.Logger)
		_ = errResponse.write(ctx)
		return
	}

	project_split := strings.Split(projectId, "-")
	if len(project_split) != 2 {
		errResponse := newErrorResponse(fmt.Sprintf("Failed to parse request body: %v", fmt.Sprintf("incorrect path %s", ctx.Request().URL)), http.StatusNotFound, nil)
		errResponse.log.write(server.Logger)
		_ = errResponse.write(ctx)
		ctx.StopExecution()
		return
	}
	projectId = "/programs/" + project_split[0] + "/projects/" + project_split[1]

	// Shouldn't have to filter on base query because rootDir_Directory edge only ever connects to the root directory
	q := gripql.V().HasLabel("ResearchStudy").Has(gripql.Eq("auth_resource_path", projectId)).OutE("rootDir_Directory").OutNull().OutNull()
	if dirPath != "/" {
		for splStr := range strings.SplitSeq(strings.Trim(dirPath, "/"), "/") {
			q = q.Has(gripql.Eq("name", splStr)).OutNull()
		}
	}

	server.Logger.Info("Executing query: %s", q.String())

	res, err := server.gripqlClient.Traversal(ctx, &gripql.GraphQuery{Graph: server.gripGraphName, Query: q.Statements})
	if err != nil {
		errResponse := newErrorResponse("internal server error", http.StatusInternalServerError, &err)
		errResponse.log.write(server.Logger)
		_ = errResponse.write(ctx)
		ctx.StopExecution()
		return
	}
	out := []any{}
	for r := range res {
		out = append(out, r.GetVertex())
	}

	jsonResponseFrom(out, 200).write(ctx)
}

func isValidPosixPath(p *string) bool {
	if strings.ContainsRune(*p, '\000') {
		return false
	}
	if !path.IsAbs(*p) {
		return false
	}
	cleaned := path.Clean(*p)
	if *p == "" || cleaned == "." {
		return false
	}
	if cleaned == ".." || strings.HasPrefix(cleaned, "/..") {
		return false
	}
	if strings.Contains(*p, "\\") {
		return false
	}
	return true
}
