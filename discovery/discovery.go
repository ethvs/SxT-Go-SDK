package discovery

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/spaceandtimelabs/SxT-Go-SDK/helpers"
)

// ListSchemas lists available namespaces in the blockchain based on scope and optional search pattern.
func ListSchemas(scope, searchPattern string) (string, string, bool) {
	endpoint := helpers.GetDiscoverEndpoint("schema") + "?scope=" + scope
	if searchPattern != "" {
		endpoint += "&searchPattern=" + searchPattern
	}
	return executeRequest(endpoint)
}

// ListTables lists tables in a given schema.
func ListTables(schema, scope, searchPattern string) (string, string, bool) {
	if errMsg, valid := helpers.CheckUpperCase(schema); !valid {
		return "", errMsg, false
	}

	endpoint := fmt.Sprintf("%s?scope=%s", helpers.GetDiscoverEndpoint("table"), scope)
	if schema != "" {
		endpoint += "&schema=" + schema
	}
	if searchPattern != "" {
		endpoint += "&searchPattern=" + searchPattern
	}
	return executeRequest(endpoint)
}

// ListColumns lists columns in a given schema and table.
func ListColumns(schema, table string) (string, string, bool) {
	return listTableInfo("column", schema, table)
}

// ListTableIndex lists table indexes in a given schema and table.
func ListTableIndex(schema, table string) (string, string, bool) {
	return listTableInfo("index", schema, table)
}

// ListTablePrimaryKey lists primary keys in a given schema and table.
func ListTablePrimaryKey(schema, table string) (string, string, bool) {
	return listTableInfo("primarykey", schema, table)
}

// ListTableRelations lists table relationships based on schema and scope.
func ListTableRelations(schema, scope string) (string, string, bool) {
	if errMsg, valid := helpers.CheckUpperCase(schema); !valid {
		return "", errMsg, false
	}
	endpoint := fmt.Sprintf("%s/relations?schema=%s&scope=%s", helpers.GetDiscoverEndpoint("table"), schema, scope)
	return executeRequest(endpoint)
}

// ListPrimaryKeyReferences lists primary key references for a schema, table, and column.
func ListPrimaryKeyReferences(schema, table, column string) (string, string, bool) {
	return listKeyReferences("primary", schema, table, column)
}

// ListForeignKeyReferences lists foreign key references for a schema, table, and column.
func ListForeignKeyReferences(schema, table, column string) (string, string, bool) {
	return listKeyReferences("foreign", schema, table, column)
}

// ListBlockchains lists all blockchains.
func ListBlockchains() (string, string, bool) {
	return listBlockchainInfo("", "")
}

// ListBlockchainSchemas lists schemas for a specific blockchain.
func ListBlockchainSchemas(chainID string) (string, string, bool) {
	return listBlockchainInfo(chainID, "schemas")
}

// ListBlockchainInformation provides metadata for a specific blockchain.
func ListBlockchainInformation(chainID string) (string, string, bool) {
	return listBlockchainInfo(chainID, "meta")
}

// ListViews lists views based on optional name and ownership parameters.
func ListViews(name, owned string) (string, string, bool) {
	var endpointBuilder strings.Builder
	endpointBuilder.WriteString(helpers.GetDiscoverEndpoint("views") + "?")

	if name != "" {
		endpointBuilder.WriteString("name=" + name)
		if owned != "" {
			endpointBuilder.WriteString("&")
		}
	}
	if owned != "" {
		endpointBuilder.WriteString("owned=" + owned)
	}
	return executeRequest(endpointBuilder.String())
}

// Helper functions
func listTableInfo(infoType, schema, table string) (string, string, bool) {
	if errMsg, valid := helpers.CheckUpperCase(schema); !valid {
		return "", errMsg, false
	}
	if errMsg, valid := helpers.CheckUpperCase(table); !valid {
		return "", errMsg, false
	}

	endpoint := fmt.Sprintf("%s/%s?schema=%s&table=%s", helpers.GetDiscoverEndpoint("table"), infoType, schema, table)
	return executeRequest(endpoint)
}

func listKeyReferences(keyType, schema, table, column string) (string, string, bool) {
	for _, field := range []string{schema, table, column} {
		if errMsg, valid := helpers.CheckUpperCase(field); !valid {
			return "", errMsg, false
		}
	}

	endpoint := fmt.Sprintf("%s/%skey?schema=%s&table=%s&column=%s", helpers.GetDiscoverEndpoint("refs"), keyType, schema, table, column)
	return executeRequest(endpoint)
}

func listBlockchainInfo(chainID, infoType string) (string, string, bool) {
	endpoint := helpers.GetDiscoverEndpoint("blockchains")
	if chainID != "" {
		endpoint = fmt.Sprintf("%s/%s/%s", endpoint, chainID, infoType)
	}
	return executeRequest(endpoint)
}

func executeRequest(endpoint string) (string, string, bool) {
	client := &http.Client{}
	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return "", fmt.Sprintf("Failed to create request: %v", err), false
	}

	accessToken := os.Getenv("accessToken")
	if accessToken == "" {
		return "", "Access token is not set", false
	}
	req.Header.Add("Authorization", "Bearer "+accessToken)

	res, err := client.Do(req)
	if err != nil {
		return "", fmt.Sprintf("Request failed: %v", err), false
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return "", fmt.Sprintf("Failed to read response body: %v", err), false
	}
	return string(body), "", true
}
