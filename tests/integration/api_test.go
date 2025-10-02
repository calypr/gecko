package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"testing"

	"github.com/calypr/gecko/gecko/adapter"
	"github.com/stretchr/testify/assert"
)

func makeRequest(method, url string, payload []byte) *http.Request {
	req, err := http.NewRequest(method, url, bytes.NewBuffer(payload))
	if err != nil {
		log.Fatal("makeRequest Err: ", err)
	}
	req.Header.Set("Content-Type", "application/json")
	return req
}

/*
func TestHealthCheck(t *testing.T) {
	resp, err := http.DefaultClient.Do(makeRequest("GET", "http://localhost:8080/health", nil))
	assert.NoError(t, err)
	buf := new(bytes.Buffer)
	buf.ReadFrom(resp.Body)
	body := buf.String()
	t.Log("health check resp body: ", body)
	assert.Contains(t, body, "Healthy")
}

func TestHandleConfigPUT(t *testing.T) {
	var configs []config.ConfigItem
	err := json.Unmarshal([]byte(fixtures.TestConfig), &configs)
	assert.NoError(t, err)
	marshalledJSON, err := json.Marshal(configs)
	assert.NoError(t, err)

	resp, err := http.DefaultClient.Do(makeRequest("PUT", "http://localhost:8080/config/123", marshalledJSON))
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	defer resp.Body.Close()

	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(resp.Body)
	assert.NoError(t, err)

	var outData map[string]any
	err = json.Unmarshal(buf.Bytes(), &outData)
	assert.NoError(t, err)
	t.Log("RESP: ", outData)

	expected200Response := map[string]any{
		"code": float64(200), "message": "ACCEPTED: 123",
	}
	assert.Equal(t, expected200Response, outData)
}

func TestHandleConfigPUTInvalidJson(t *testing.T) {
	resp, err := http.DefaultClient.Do(makeRequest("PUT", "http://localhost:8080/config/123", []byte("invalid json")))
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	defer resp.Body.Close()

	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(resp.Body)
	assert.NoError(t, err)

	var errData map[string]any
	err = json.Unmarshal(buf.Bytes(), &errData)
	t.Log("BYTES: ", string(buf.Bytes()))
	assert.NoError(t, err)

	expectedErrorResponse := map[string]any{
		"error": map[string]any{
			"code":    float64(400),
			"message": "Invalid JSON format",
		},
	}
	assert.Equal(t, expectedErrorResponse, errData)
}

func TestHandleConfigPUTInvalidObject(t *testing.T) {
	marshalledJSON, err := json.Marshal(map[string]any{"foo": "bar"})
	assert.NoError(t, err)
	resp, err := http.DefaultClient.Do(makeRequest("PUT", "http://localhost:8080/config/123", marshalledJSON))

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	defer resp.Body.Close()

	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(resp.Body)
	assert.NoError(t, err)

	var errData map[string]any
	err = json.Unmarshal(buf.Bytes(), &errData)
	assert.NoError(t, err)

	t.Log("BYTES: ", string(buf.Bytes()))
	expectedErrorResponse := map[string]any{
		"error": map[string]any{
			"code":    float64(400),
			"message": "body data unmarshal failed: json: cannot unmarshal object into Go value of type []config.ConfigItem",
		},
	}
	assert.Equal(t, expectedErrorResponse, errData)
}

func TestHandleConfigGET(t *testing.T) {
	var configs []config.ConfigItem
	err := json.Unmarshal([]byte(fixtures.TestConfig), &configs)

	payloadBytes, err := json.Marshal(configs)
	assert.NoError(t, err)

	_, err = http.DefaultClient.Do(makeRequest("PUT", "http://localhost:8080/config/123", payloadBytes))
	assert.NoError(t, err)

	resp, err := http.DefaultClient.Do(makeRequest("GET", "http://localhost:8080/config/123", nil))
	assert.NoError(t, err)

	buf := new(bytes.Buffer)
	buf.ReadFrom(resp.Body)
	resp.Body.Close()
	var outdata map[string]any
	json.Unmarshal(buf.Bytes(), &outdata)

	var Resconfigs []config.ConfigItem
	data, _ := json.Marshal(outdata["content"])
	err = json.Unmarshal(data, &Resconfigs)
	assert.NoError(t, err)

	resp.Body.Close()
}

func TestHandle404ConfigGet(t *testing.T) {
	resp, err := http.DefaultClient.Do(makeRequest("GET", "http://localhost:8080/config/404config", nil))
	assert.NoError(t, err)
	assert.Equal(t, resp.StatusCode, 404)
}

func TestHandle404ConfigDelete(t *testing.T) {
	resp, err := http.DefaultClient.Do(makeRequest("DELETE", "http://localhost:8080/config/404config", nil))
	assert.NoError(t, err)
	assert.Equal(t, resp.StatusCode, 404)
}

func TestHandleConfigDeleteOK(t *testing.T) {
	var configs []config.ConfigItem
	err := json.Unmarshal([]byte(fixtures.TestConfig), &configs)
	payloadBytes, err := json.Marshal(configs)
	assert.NoError(t, err)
	_, err = http.DefaultClient.Do(makeRequest("PUT", "http://localhost:8080/config/testdelete", payloadBytes))
	assert.NoError(t, err)

	resp, err := http.DefaultClient.Do(makeRequest("DELETE", "http://localhost:8080/config/testdelete", nil))
	assert.NoError(t, err)
	assert.Equal(t, resp.StatusCode, 200)

	resp, err = http.DefaultClient.Do(makeRequest("GET", "http://localhost:8080/config/testdelete", nil))
	assert.NoError(t, err)
	assert.Equal(t, resp.StatusCode, 404)
}
*/

func ptr[T any](v T) *T {
	return &v
}

func generateRandomFloats(n int) []float32 {
	randomFloats := make([]float32, n)
	for i := range n {
		randomFloats[i] = rand.Float32()
	}
	return randomFloats
}

const testCollectionName = "test_collection_gecko"
const vectorEndpoint = "http://localhost:8080/vector/collections"
const queryEndpoint = "http://localhost:8080/vector/collections/%s/points/search"
const VECTOR_NAME = "test_vector"

func cleanupCollection(t *testing.T, name string) {
	t.Helper()
	url := fmt.Sprintf("%s/%s", vectorEndpoint, name)
	_, err := http.DefaultClient.Do(makeRequest(http.MethodDelete, url, nil))
	if err != nil {
		t.Logf("Cleanup (ignorable error): Failed to delete collection %s: %v", name, err)
	}
}

func TestQdrantCollectionWorkflow(t *testing.T) {
	cleanupCollection(t, testCollectionName)
	pointsEndpoint := fmt.Sprintf("%s/%s/points", vectorEndpoint, testCollectionName)
	// Test CreateCollection (PUT /vector/collections/{collection})
	t.Run("CreateCollection_OK", func(t *testing.T) {
		url := fmt.Sprintf("%s/%s", vectorEndpoint, testCollectionName)

		// Matches adapter/types.go::CreateCollectionRequest
		createPayloadJSON := map[string]any{
			"vectors": map[string]any{
				VECTOR_NAME: map[string]any{
					"size":     128,
					"distance": "Cosine",
				},
			},
		}

		marshalledJSON, err := json.Marshal(createPayloadJSON)
		assert.NoError(t, err)

		resp, err := http.DefaultClient.Do(makeRequest(http.MethodPut, url, marshalledJSON))
		assert.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode, "Expected 200 OK for successful collection creation")

		var respData map[string]any
		buf := new(bytes.Buffer)
		_, _ = buf.ReadFrom(resp.Body)
		_ = json.Unmarshal(buf.Bytes(), &respData)

		assert.Equal(t, true, respData["result"], "Expected result: true in response body")
	})

	// Test GetCollectionInfo (GET /vector/collections/{collection})
	t.Run("GetCollection_OK", func(t *testing.T) {
		url := fmt.Sprintf("%s/%s", vectorEndpoint, testCollectionName)
		resp, err := http.DefaultClient.Do(makeRequest(http.MethodGet, url, nil))
		assert.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode, "Expected 200 OK for getting collection info")

		var respData map[string]any
		buf := new(bytes.Buffer)
		_, _ = buf.ReadFrom(resp.Body)
		_ = json.Unmarshal(buf.Bytes(), &respData)

		assert.Contains(t, respData, "config", "Response should contain the collection config data")
	})

	// Test ListCollections (GET /vector/collections)
	t.Run("ListCollections_OK", func(t *testing.T) {
		resp, err := http.DefaultClient.Do(makeRequest(http.MethodGet, vectorEndpoint, nil))
		assert.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode, "Expected 200 OK for listing collections")

		var respData map[string]any
		buf := new(bytes.Buffer)
		_, _ = buf.ReadFrom(resp.Body)
		_ = json.Unmarshal(buf.Bytes(), &respData)

		collections, ok := respData["result"].([]any)
		assert.True(t, ok, "result field should be a list")
		assert.True(t, len(collections) > 0)

	})

	t.Run("UpsertPoints_OK", func(t *testing.T) {
		// Matches adapter/types.go::UpsertRequest
		upsertPayload := map[string]any{
			"points": []map[string]any{
				{
					"id": "c3fb3d5c-e423-46ba-a47a-9ff97b94fc50",
					"payload": map[string]any{
						"color": "red",
					},
					"vector_name": VECTOR_NAME,
					"vector":      generateRandomFloats(128),
				},
				{
					"id": "5eb1d065-e222-4e20-a821-954d518844e7",
					"payload": map[string]any{
						"color": "green",
					},
					"vector_name": VECTOR_NAME,
					"vector":      generateRandomFloats(128),
				},
				{
					"id": "1cf900d5-1799-4baa-ac96-ecf7cfaeb94c",
					"payload": map[string]any{
						"color": "blue",
					},
					"vector_name": VECTOR_NAME,
					"vector":      generateRandomFloats(128),
				},
			},
		}

		marshalledJSON, err := json.Marshal(upsertPayload)
		assert.NoError(t, err)

		resp, err := http.DefaultClient.Do(makeRequest(http.MethodPut, pointsEndpoint, marshalledJSON))
		assert.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode, "Expected 200 OK for successful upsert")

		var respData map[string]any
		buf := new(bytes.Buffer)
		_, err = buf.ReadFrom(resp.Body)
		assert.NoError(t, err, "Failed to read response body")
		err = json.Unmarshal(buf.Bytes(), &respData)
		assert.NoError(t, err, "Failed to unmarshal response")
		assert.Equal(t, "Completed", respData["status"], "Expected result: Acknowledged in response body")
	})

	t.Run("GetPoint_OK", func(t *testing.T) {
		// Just get the point don't worry about doing a query
		pointID := "c3fb3d5c-e423-46ba-a47a-9ff97b94fc50"
		url := fmt.Sprintf("%s/%s", pointsEndpoint, pointID)

		resp, err := http.DefaultClient.Do(makeRequest(http.MethodGet, url, nil))
		assert.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode, "Expected 200 OK for getting the point")
		var respData []map[string]any
		buf := new(bytes.Buffer)
		_, _ = buf.ReadFrom(resp.Body)
		_ = json.Unmarshal(buf.Bytes(), &respData)

		assert.NotEmpty(t, respData, "Response should contain the point data")
		assert.Equal(
			t,
			"c3fb3d5c-e423-46ba-a47a-9ff97b94fc50",
			respData[0]["id"].(map[string]any)["PointIdOptions"].(map[string]any)["Uuid"],
			"Expected point ID to be c3fb3d5c-e423-46ba-a47a-9ff97b94fc50",
		)
	})

	t.Run("QueryPoints_Success", func(t *testing.T) {
		url := fmt.Sprintf(queryEndpoint, testCollectionName)
		requestBody := adapter.QueryPointsRequest{
			LookupID:   ptr("c3fb3d5c-e423-46ba-a47a-9ff97b94fc50"),
			Limit:      100,
			VectorName: VECTOR_NAME,
			WithVector: ptr(true),
		}

		bodyBytes, err := json.Marshal(requestBody)
		if err != nil {
			t.Fatalf("Marshal failed on %#v", requestBody)
		}

		resp, err := http.DefaultClient.Do(makeRequest(http.MethodPost, url, bodyBytes))
		assert.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode, "Expected 200 OK for successful query")

		var actualResponse []map[string]any
		buf := new(bytes.Buffer)
		_, err = buf.ReadFrom(resp.Body)

		assert.NoError(t, err, "Failed to read response body")
		err = json.Unmarshal(buf.Bytes(), &actualResponse)
		//t.Log("RESP TWO: ", buf.String())

		assert.NoError(t, err, "Failed to unmarshal response")
		assert.Len(t, actualResponse, 2)

	})

	t.Run("QueryPoints_MissingVector_BadRequest", func(t *testing.T) {
		url := fmt.Sprintf(queryEndpoint, testCollectionName)
		requestBody := adapter.QueryPointsRequest{
			Query: []float32{},
			Limit: 5,
		}
		bodyBytes, _ := json.Marshal(requestBody)
		resp, err := http.DefaultClient.Do(makeRequest(http.MethodPost, url, bodyBytes))
		assert.NoError(t, err)

		defer resp.Body.Close()

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		var errResp map[string]any
		err = json.NewDecoder(resp.Body).Decode(&errResp)
		assert.NoError(t, err)
		assert.Contains(t, errResp["error"].(map[string]any)["message"], "invalid query parameter: must specify either 'query' vector or 'lookup_id'")
	})

	t.Run("DeletePoints_OK", func(t *testing.T) {
		// Matches adapter/types.go::DeletePoints
		deletePayloadJSON := map[string]any{
			"points": []string{"c3fb3d5c-e423-46ba-a47a-9ff97b94fc50"},
		}
		marshalledJSON, err := json.Marshal(deletePayloadJSON)
		assert.NoError(t, err)

		resp, err := http.DefaultClient.Do(makeRequest(http.MethodPost, fmt.Sprintf("%s/delete", pointsEndpoint), marshalledJSON))
		assert.NoError(t, err)
		defer resp.Body.Close()

		var respData map[string]any
		buf := new(bytes.Buffer)
		_, err = buf.ReadFrom(resp.Body)
		_ = json.Unmarshal(buf.Bytes(), &respData)

		assert.Equal(t, http.StatusOK, resp.StatusCode, "Expected 200 OK for successful delete")

		pointID := "c3fb3d5c-e423-46ba-a47a-9ff97b94fc50"
		url := fmt.Sprintf("%s/%s", pointsEndpoint, pointID)
		resp, err = http.DefaultClient.Do(makeRequest(http.MethodGet, url, nil))
		assert.NoError(t, err, "WHAT IS RESP: %v", resp.Status)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusNotFound, resp.StatusCode, "Expected 404 Not Found after deleting point")
	})

	t.Run("DeleteCollection_OK", func(t *testing.T) {
		url := fmt.Sprintf("%s/%s", vectorEndpoint, testCollectionName)
		resp, err := http.DefaultClient.Do(makeRequest(http.MethodDelete, url, nil))
		assert.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode, "Expected 200 OK for successful deletion")

		resp, err = http.DefaultClient.Do(makeRequest(http.MethodGet, url, nil))
		assert.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusNotFound, resp.StatusCode, "Expected an error (e.g. 500) after deleting collection and trying to GET it")
	})

}
