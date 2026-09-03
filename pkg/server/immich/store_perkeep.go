// The Perkeep-backed store behind the Immich server.
//
// It holds the real Perkeep dependencies (search index + blob storage) and
// answers the read-only queries the Immich handlers need. The query methods
// are marked TODO[STUB] until the exact search.API queries are wired.
package immich

import (
	"context"
	"fmt"
	"net/http"

	openapi_types "github.com/oapi-codegen/runtime/types"

	"perkeep.org/pkg/blobserver"
	"perkeep.org/pkg/search"
)

// store holds the Perkeep query + blobserving dependencies.
type store struct {
	// handler is the local search index for metadata queries.
	handler *search.Handler
	// storage is the blob source for byte streaming (originals + thumbnails).
	storage blobserver.Storage
}

// newStore builds the store from a search index + blob storage.
func newStore(handler *search.Handler, storage blobserver.Storage) *store {
	return &store{handler: handler, storage: storage}
}

// ListAlbums returns the metadata of every album.
//
// TODO[STUB]: decide what marks a Perkeep object as an "album" (tag? directory
// permanode listing file permanodes? naming convention?) - see PLAN open-Q #2.
func (s *store) ListAlbums(ctx context.Context) ([]AlbumResponseDto, error) {
	return nil, fmt.Errorf("TODO[STUB] ListAlbums not implemented")
}

// GetAlbum returns one album by its Immich (derived) id.
func (s *store) GetAlbum(ctx context.Context, id openapi_types.UUID) (*AlbumResponseDto, error) {
	return nil, fmt.Errorf("TODO[STUB] GetAlbum not implemented")
}

// TimeBuckets returns one bucket per distinct photo day, oldest first.
func (s *store) TimeBuckets(ctx context.Context) ([]TimeBucketsResponseDto, error) {
	return nil, fmt.Errorf("TODO[STUB] TimeBuckets not implemented")
}

// TimeBucket returns the photos in one date bucket (YYYY-MM-DD). When
// params.AlbumId is set it is restricted to that album (the UI opens an album
// through here).
func (s *store) TimeBucket(ctx context.Context, params GetTimeBucketParams) ([]AssetResponseDto, error) {
	return nil, fmt.Errorf("TODO[STUB] TimeBucket not implemented")
}

// GetAsset returns one asset's metadata by its Immich (derived) id.
func (s *store) GetAsset(ctx context.Context, id openapi_types.UUID) (*AssetResponseDto, error) {
	return nil, fmt.Errorf("TODO[STUB] GetAsset not implemented")
}

// ServeOriginal streams the original bytes of an asset to w.
func (s *store) ServeOriginal(ctx context.Context, w http.ResponseWriter, r *http.Request, id openapi_types.UUID) error {
	return fmt.Errorf("TODO[STUB] ServeOriginal not implemented")
}

// ServeThumbnail streams the thumbnail bytes of an asset to w. Reuse Perkeep's
// thumbcache where possible (see PLAN open-Q #8).
func (s *store) ServeThumbnail(ctx context.Context, w http.ResponseWriter, r *http.Request, id openapi_types.UUID) error {
	return fmt.Errorf("TODO[STUB] ServeThumbnail not implemented")
}

// SearchAssets returns assets matching a metadata query.
func (s *store) SearchAssets(ctx context.Context) (*SearchResponseDto, error) {
	return nil, fmt.Errorf("TODO[STUB] SearchAssets not implemented")
}

// AssetStatistics returns aggregate image/video counts.
func (s *store) AssetStatistics(ctx context.Context) (*AssetStatsResponseDto, error) {
	return nil, fmt.Errorf("TODO[STUB] AssetStatistics not implemented")
}

// Totals returns (photoCount, totalBytesUsed, error).
func (s *store) Totals(ctx context.Context) (int, int, error) {
	return 0, 0, fmt.Errorf("TODO[STUB] Totals not implemented")
}
