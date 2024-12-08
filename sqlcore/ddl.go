package sqlcore

import (
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// CreateTable creates a new table in a given namespace.
// accessType: can be public, permissioned, or encrypted. Refer to: https://docs.spaceandtime.io/docs/secure-your-table
func CreateTable(sqlText, accessType, originApp string, biscuitArray []string, publicKey ed25519.PublicKey) (string, bool) {
	if accessType == "" || !isValidAccessType(accessType) {
		return "Invalid access type", false
	}

	sqlTextWithConfiguration := fmt.Sprintf("%s WITH \"public_key=%x,access_type=%s\"", sqlText, publicKey, accessType)
	return DDL(sqlTextWithConfiguration, originApp, biscuitArray)
}

// DDL performs Data Definition Language (DDL) operations like ALTER and DROP.
func DDL(sqlText, originApp string, biscuitArray []string) (string, bool) {
	request, err := createResourceConfigurationRequest(sqlText, originApp, biscuitArray)
	if err != nil {
		return fmt.Sprintf("Failed to create request: %v", err), false
	}

	client := http.Client{}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Sprintf("Failed to execute request: %v", err), false
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return fmt.Sprintf("Failed to read response body: %v", err), false
	}

	if response.StatusCode != http.StatusOK {
		return fmt.Sprintf("Request failed with status %d: %s", response.StatusCode, string(body)), false
	}

	return "", true
}

// createResourceConfigurationRequest builds an HTTP request for resource configuration.
func createResourceConfigurationRequest(sqlText, originApp string, biscuitArray []string) (*http.Request, error) {
	postBody, err := json.Marshal(map[string]interface{}{
		"biscuits": biscuitArray,
		"sqlText":  sqlText,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request body: %v", err)
	}

	return createRequest("ddl", originApp, postBody)
}

// CreateSchema creates a new schema in the namespace.
// It uses the same logic as DDL.
func CreateSchema(sqlText, originApp string, biscuitArray []string) (string, bool) {
	return DDL(sqlText, originApp, biscuitArray)
}

// createRequest constructs a generic HTTP request for the given action.
func createRequest(action, originApp string, body []byte) (*http.Request, error) {
	url := fmt.Sprintf("https://api.example.com/%s", action) // Replace with actual API base URL.
	req, err := http.NewRequest("POST", url, strings.NewReader(string(body)))
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if originApp != "" {
		req.Header.Set("X-Origin-App", originApp)
	}

	return req, nil
}

// isValidAccessType validates if the access type is supported.
func isValidAccessType(accessType string) bool {
	supportedAccessTypes := []string{"public", "permissioned", "encrypted"}
	for _, t := range supportedAccessTypes {
		if accessType == t {
			return true
		}
	}
	return false
}
