package api

import (
	"fmt"
	"net/http"
	"path"
	"strings"

	"github.com/bmeg/grip-graphql/middleware"
	"github.com/bmeg/grip/gripql"
	"github.com/calypr/gecko/apierror"
	"github.com/calypr/gecko/internal/authz"
	"github.com/calypr/gecko/internal/httputil"
	"github.com/gofiber/fiber/v3"
)

func (handler *Handler) registerDirectoryHandlers(app fiber.Router, authMiddleware fiber.Handler) {
	app.Get("/dir", handler.handleListProjects)
	app.Get("/dir/:projectId", authMiddleware, handler.handleDirGet)
}

// handleListProjects godoc
// @Summary List authorized projects
// @Description Retrieve the set of projects visible to the current user.
// @Tags Directory
// @Produce json
// @Success 200 {array} string "Project resource paths"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 500 {object} ErrorResponse "Server error"
// @Router /dir [get]
func (handler *Handler) handleListProjects(ctx fiber.Ctx) error {
	projs, errResponse := authz.ProjectsFromToken(ctx, &middleware.ProdJWTHandler{}, "read", "*")
	if errResponse != nil {
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	q := buildListProjectsQuery(projs)
	res, err := handler.gripqlClient.Traversal(ctx, &gripql.GraphQuery{Graph: handler.gripGraphName, Query: q.Statements})
	if err != nil {
		errResponse = httputil.NewError(apierror.TypeGraphQueryFailed, "graph query failed", http.StatusInternalServerError, nil, &err)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	out := []string{}
	for r := range res {
		renda, ok := r.GetRender().GetStructValue().AsMap()["project"].(string)
		if ok {
			out = append(out, renda)
		}
	}
	return httputil.JSON(out, http.StatusOK).Write(ctx)
}

// handleDirGet godoc
// @Summary Retrieve directory information for a project
// @Description Retrieve directory details for the given project ID and directory path.
// @Tags Directory
// @Produce json
// @Param projectId path string true "Project ID (format: program-project)"
// @Param directory query string true "Directory path (e.g. /data/my-dir)"
// @Success 200 {array} map[string]interface{} "Directory information"
// @Failure 400 {object} ErrorResponse "Invalid request body or directory path"
// @Failure 403 {object} ErrorResponse "User is not allowed on the resource path"
// @Failure 500 {object} ErrorResponse "Server error"
// @Router /dir/{projectId} [get]
func (handler *Handler) handleDirGet(ctx fiber.Ctx) error {
	projectID := ctx.Params("projectId")
	dirPath := ctx.Query("directory")
	if dirPath == "" || !isValidPosixPath(&dirPath) {
		errResponse := httputil.NewError(apierror.TypeInvalidDirectory, fmt.Sprintf("Invalid or missing Directory path: %s", dirPath), http.StatusBadRequest, map[string]any{"directory": dirPath}, nil)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}

	projectSplit := strings.Split(projectID, "-")
	if len(projectSplit) != 2 {
		errResponse := httputil.NewError(apierror.TypeInvalidProjectID, fmt.Sprintf("Failed to parse request body: %v", fmt.Sprintf("incorrect path %s", ctx.Path())), http.StatusNotFound, map[string]any{"project_id": projectID}, nil)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	projectID = "/programs/" + projectSplit[0] + "/projects/" + projectSplit[1]

	q := buildDirGetQuery(projectID, dirPath)
	res, err := handler.gripqlClient.Traversal(ctx, &gripql.GraphQuery{Graph: handler.gripGraphName, Query: q.Statements})
	if err != nil {
		errResponse := httputil.NewError(apierror.TypeGraphQueryFailed, "graph query failed", http.StatusInternalServerError, nil, &err)
		errResponse.WriteLog(handler.logger)
		return errResponse.Write(ctx)
	}
	out := []any{}
	for r := range res {
		out = append(out, r.GetVertex())
	}
	return httputil.JSON(out, http.StatusOK).Write(ctx)
}

func isValidPosixPath(p *string) bool {
	if strings.ContainsRune(*p, 000) || !path.IsAbs(*p) || strings.Contains(*p, "\\") {
		return false
	}
	cleaned := path.Clean(*p)
	if *p == "" || cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, "/..") {
		return false
	}
	return true
}

func buildListProjectsQuery(projs []any) *gripql.Query {
	return gripql.V().
		HasLabel("ResearchStudy").
		Has(gripql.Within("auth_resource_path", projs...)).
		As("project").
		OutE("rootDir_Directory").
		Select("project").
		Distinct("auth_resource_path").
		Render(map[string]any{"project": "$project.auth_resource_path"})
}

func buildDirGetQuery(projectID, dirPath string) *gripql.Query {
	q := gripql.V().HasLabel("ResearchStudy").Has(gripql.Eq("auth_resource_path", projectID)).OutE("rootDir_Directory").OutNull().OutNull()
	if dirPath != "/" {
		for splStr := range strings.SplitSeq(strings.Trim(dirPath, "/"), "/") {
			q = q.Has(gripql.Eq("name", splStr)).Has(gripql.Eq("auth_resource_path", projectID)).OutNull()
		}
	} else {
		q = q.Has(gripql.Eq("auth_resource_path", projectID))
	}
	return q
}
