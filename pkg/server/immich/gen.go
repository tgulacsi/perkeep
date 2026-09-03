//go:generate curl -o immich-openapi-specs.json -sS -m30 -L https://github.com/immich-app/immich/raw/refs/heads/main/open-api/immich-openapi-specs.json
//go:generate go tool oapi-codegen -generate types,chi-server -o immich.go -package immich immich-openapi-specs.json
//

package immich
