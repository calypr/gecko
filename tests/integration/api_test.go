package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/calypr/gecko/gecko/config"
	"github.com/calypr/gecko/tests/fixtures"
	"github.com/stretchr/testify/assert"
)

func makeRequest(method, url string, payload []byte) *http.Request {
	req, _ := http.NewRequest(method, url, bytes.NewBuffer(payload))
	req.Header.Set("Content-Type", "application/json")
	return req
}

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

// --------------------------------------------------------------------------------

const testCollectionName = "test_collection_gecko"
const vectorEndpoint = "http://localhost:8080/vector/collections"

func cleanupCollection(t *testing.T, name string) {
	t.Helper()
	url := fmt.Sprintf("%s/%s", vectorEndpoint, name)
	_, err := http.DefaultClient.Do(makeRequest("DELETE", url, nil))
	if err != nil {
		t.Logf("Cleanup (ignorable error): Failed to delete collection %s: %v", name, err)
	}
}

func TestQdrantCollectionWorkflow(t *testing.T) {
	cleanupCollection(t, testCollectionName)
	defer cleanupCollection(t, testCollectionName)

	// Test CreateCollection (PUT /vector/collections/{collection})
	t.Run("CreateCollection_OK", func(t *testing.T) {
		url := fmt.Sprintf("%s/%s", vectorEndpoint, testCollectionName)

		createPayloadJSON := map[string]any{
			"vectors_config": map[string]any{
				"params": map[string]any{
					"size":     128,
					"distance": "Cosine", // Use string for Distance enum value
				},
			},
		}

		marshalledJSON, err := json.Marshal(createPayloadJSON)
		assert.NoError(t, err)

		resp, err := http.DefaultClient.Do(makeRequest("PUT", url, marshalledJSON))
		assert.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode, "Expected 200 OK for successful collection creation")

		var respData map[string]any
		buf := new(bytes.Buffer)
		_, _ = buf.ReadFrom(resp.Body)
		_ = json.Unmarshal(buf.Bytes(), &respData)

		assert.Equal(t, true, respData["result"], "Expected result: true in response body")
	})

	// 3. Test GetCollectionInfo (GET /vector/collections/{collection})
	t.Run("GetCollection_OK", func(t *testing.T) {
		url := fmt.Sprintf("%s/%s", vectorEndpoint, testCollectionName)
		resp, err := http.DefaultClient.Do(makeRequest("GET", url, nil))
		assert.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode, "Expected 200 OK for getting collection info")

		var respData map[string]any
		buf := new(bytes.Buffer)
		_, _ = buf.ReadFrom(resp.Body)
		_ = json.Unmarshal(buf.Bytes(), &respData)

		assert.Contains(t, respData, "config", "Response should contain the collection config data")
	})

	// 4. Test ListCollections (GET /vector/collections)
	t.Run("ListCollections_OK", func(t *testing.T) {
		resp, err := http.DefaultClient.Do(makeRequest("GET", vectorEndpoint, nil))
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

	// 5. Test DeleteCollection (DELETE /vector/collections/{collection})
	t.Run("DeleteCollection_OK", func(t *testing.T) {
		url := fmt.Sprintf("%s/%s", vectorEndpoint, testCollectionName)
		resp, err := http.DefaultClient.Do(makeRequest("DELETE", url, nil))
		assert.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode, "Expected 200 OK for successful deletion")

		// Verify deletion
		resp, err = http.DefaultClient.Do(makeRequest("GET", url, nil))
		assert.NoError(t, err)
		defer resp.Body.Close()

		// Qdrant typically returns 404 or a "not found" message on collection info if deleted
		assert.Equal(t, http.StatusNotFound, resp.StatusCode, "Expected an error (e.g. 500) after deleting collection and trying to GET it")
	})
}

func TestQdrantPointsWorkflow(t *testing.T) {
	setupCollectionName := "test_points_collection"
	cleanupCollection(t, setupCollectionName)
	defer cleanupCollection(t, setupCollectionName)

	url := fmt.Sprintf("%s/%s", vectorEndpoint, setupCollectionName)
	createPayloadJSON := map[string]any{
		"vectors_config": map[string]any{
			"params": map[string]any{
				"size":     4,
				"distance": "Cosine",
			},
		},
	}

	marshalledJSON, _ := json.Marshal(createPayloadJSON)
	_, err := http.DefaultClient.Do(makeRequest("PUT", url, marshalledJSON))
	assert.NoError(t, err)

	pointsEndpoint := fmt.Sprintf("%s/%s/points", vectorEndpoint, setupCollectionName)

	t.Run("UpsertPoints_OK", func(t *testing.T) {
		upsertPayloadJSON := map[string]interface{}{
			"points": []map[string]interface{}{
				{
					"id":      "1",                           // Use integer for PointId_Num
					"vectors": []float64{0.1, 0.2, 0.3, 0.4}, // Use "vectors" to match PointStruct
					"payload": map[string]interface{}{
						"id": "d1", // String value for Value_StringValue
					},
				},
			}}

		marshalledJSON, err := json.Marshal(upsertPayloadJSON)
		assert.NoError(t, err)

		resp, err := http.DefaultClient.Do(makeRequest("PUT", pointsEndpoint, marshalledJSON))
		assert.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode, "Expected 200 OK for successful upsert")

		var respData map[string]interface{}
		buf := new(bytes.Buffer)
		_, err = buf.ReadFrom(resp.Body)
		assert.NoError(t, err, "Failed to read response body")
		err = json.Unmarshal(buf.Bytes(), &respData)
		assert.NoError(t, err, "Failed to unmarshal response")
		t.Log("RESP: ", buf.String())
		assert.Equal(t, true, respData["result"], "Expected result: true in response body")
	})

	t.Run("GetPoint_OK", func(t *testing.T) {
		pointID := "1"
		url := fmt.Sprintf("%s/%s", pointsEndpoint, pointID)

		resp, err := http.DefaultClient.Do(makeRequest("GET", url, nil))
		assert.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode, "Expected 200 OK for getting the point")

		var respData []map[string]any
		buf := new(bytes.Buffer)
		_, _ = buf.ReadFrom(resp.Body)
		_ = json.Unmarshal(buf.Bytes(), &respData)

		t.Log("RESP: ", buf.String())
		assert.NotEmpty(t, respData, "Response should contain the point data")
		assert.Equal(t, float64(1), respData[0]["id"], "Expected point ID to be 1")
	})

	t.Run("DeletePoints_OK", func(t *testing.T) {
		deletePayloadJSON := map[string]any{
			"points": []float64{1}, // For deleting a specific point ID
			"wait":   true,
		}
		marshalledJSON, err := json.Marshal(deletePayloadJSON)
		assert.NoError(t, err)

		resp, err := http.DefaultClient.Do(makeRequest("POST", fmt.Sprintf("%s/delete", pointsEndpoint), marshalledJSON))
		assert.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode, "Expected 200 OK for successful delete")

		pointID := "1"
		url := fmt.Sprintf("%s/%s", pointsEndpoint, pointID)
		resp, err = http.DefaultClient.Do(makeRequest("GET", url, nil))
		assert.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusNotFound, resp.StatusCode, "Expected 404 Not Found after deleting point")
	})

}
