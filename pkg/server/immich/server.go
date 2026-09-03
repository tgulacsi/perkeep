// Server implements the Immich HTTP API on top of a Perkeep store, embedding
// the generated Unimplemented so every endpoint we do not override answers 501.
package immich

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	openapi_types "github.com/oapi-codegen/runtime/types"
)

// Sentinel errors returned by handlers and the store. writeError maps them to
// HTTP statuses; any other error becomes 500.
var (
	errNotFound = errors.New("not found")
	errBadParam = errors.New("bad parameter")
)

// writeError writes a minimal JSON error body with the status implied by err.
// TODO[STUB]: Immich expects structured error bodies; confirm the client
// tolerates a plain status + {"error": ...} body.
func writeError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, errNotFound):
		status = http.StatusNotFound
	case errors.Is(err, errBadParam):
		status = http.StatusBadRequest
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}

// writeJSON marshals v and writes it with the given status.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// Version reported by GetServerVersion; the Immich UI refuses to talk to a
// server it cannot version.
// TODO[STUB]: pull the real version from perkeep.org/pkg/buildinfo.
const version = "0.1.0"

// The single implicit owner this MVP serves (auth is faked).
const (
	ownerUserName  = "perkeep"
	ownerUserEmail = "perkeep@localhost"
)

// userID is the stable UUID reported for the singleton owner. It is unrelated
// to any blobref.
// FIXME: arbitrary but stable id; must not collide with asset/album ids.
var userID = openapi_types.UUID{
	0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x41, 0x11,
	0x81, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11,
}

// Server implements the served endpoints; everything else falls through to the
// embedded Unimplemented and answers 501.
type Server struct {
	Unimplemented
	st *store
}

// NewServer returns a Server backed by the Perkeep store st.
func NewServer(st *store) *Server {
	return &Server{st: st}
}

// userResponseDto builds the singleton owner user DTO.
func userResponseDto() UserResponseDto {
	return UserResponseDto{
		Id:    userID,
		Name:  ownerUserName,
		Email: ownerUserEmail,
	}
}

// ---- handshake endpoints ----

// PingServer answers Immich's liveness probe.
func (s *Server) PingServer(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, ServerPingResponse{Res: "pong"})
}

// GetServerVersion answers Immich's version handshake.
func (s *Server) GetServerVersion(w http.ResponseWriter, r *http.Request) {
	var major, minor, patch int
	_, _ = fmt.Sscanf(version, "%d.%d.%d", &major, &minor, &patch)
	writeJSON(w, http.StatusOK, ServerVersionResponseDto{
		Major: major, Minor: minor, Patch: patch,
	})
}

// GetServerConfig reports the server is initialized + onboarded so the UI
// proceeds to the album/photo views.
func (s *Server) GetServerConfig(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, ServerConfigDto{
		IsInitialized: true,
		IsOnboarded:   true,
	})
}

// GetSupportedMediaTypes lists the image MIME types the MVP can serve.
func (s *Server) GetSupportedMediaTypes(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, ServerMediaTypesResponseDto{
		Image: []string{"image/jpeg", "image/png", "image/gif", "image/webp", "image/tiff"},
	})
}

// GetMyUser returns the implicit owner (fake auth handshake).
func (s *Server) GetMyUser(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, userResponseDto())
}

// GetAuthStatus reports the fake session is valid, with no password/PIN.
func (s *Server) GetAuthStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, AuthStatusResponseDto{
		IsElevated: true,
	})
}

// ---- albums ----

// GetAllAlbums lists the albums found in the store.
//
// TODO[STUB]: honor params filters (AssetId, Id, IsOwned, IsShared, Name) once
// the album model is decided (PLAN open-Q #2).
func (s *Server) GetAllAlbums(w http.ResponseWriter, r *http.Request, params GetAllAlbumsParams) {
	albums, err := s.st.ListAlbums(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, albums)
}

// GetAlbumInfo returns a single album.
func (s *Server) GetAlbumInfo(w http.ResponseWriter, r *http.Request, id openapi_types.UUID, params GetAlbumInfoParams) {
	album, err := s.st.GetAlbum(r.Context(), id)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, album)
}

// GetAlbumStatistics reports ownership counts. With a single implicit owner,
// every album is owned and non-shared.
func (s *Server) GetAlbumStatistics(w http.ResponseWriter, r *http.Request) {
	albums, err := s.st.ListAlbums(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, AlbumStatisticsResponseDto{
		Owned: len(albums), NotShared: len(albums),
	})
}

// ---- photos / timeline ----

// GetTimeBuckets groups photos into date buckets for the timeline.
func (s *Server) GetTimeBuckets(w http.ResponseWriter, r *http.Request, params GetTimeBucketsParams) {
	buckets, err := s.st.TimeBuckets(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, buckets)
}

// GetTimeBucket returns the photos in one date bucket. The UI opens an album
// by calling this with AlbumId set, so this is the workhorse for album photos.
func (s *Server) GetTimeBucket(w http.ResponseWriter, r *http.Request, params GetTimeBucketParams) {
	assets, err := s.st.TimeBucket(r.Context(), params)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, assets)
}

// GetAssetInfo returns metadata for one asset.
func (s *Server) GetAssetInfo(w http.ResponseWriter, r *http.Request, id openapi_types.UUID, params GetAssetInfoParams) {
	asset, err := s.st.GetAsset(r.Context(), id)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, asset)
}

// DownloadAsset streams the original bytes of an asset.
func (s *Server) DownloadAsset(w http.ResponseWriter, r *http.Request, id openapi_types.UUID, params DownloadAssetParams) {
	if err := s.st.ServeOriginal(r.Context(), w, r, id); err != nil {
		writeError(w, err)
	}
}

// ViewAsset serves the thumbnail bytes of an asset.
func (s *Server) ViewAsset(w http.ResponseWriter, r *http.Request, id openapi_types.UUID, params ViewAssetParams) {
	if err := s.st.ServeThumbnail(r.Context(), w, r, id); err != nil {
		writeError(w, err)
	}
}

// ---- search ----

// SearchAssets returns assets matching a metadata query (body already decoded
// by the generated wrapper).
func (s *Server) SearchAssets(w http.ResponseWriter, r *http.Request, params SearchAssetsParams) {
	res, err := s.st.SearchAssets(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// SearchAssetStatistics reports aggregate asset counts.
func (s *Server) SearchAssetStatistics(w http.ResponseWriter, r *http.Request) {
	stats, err := s.st.AssetStatistics(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

// GetServerStatistics reports aggregate photo counts and usage.
func (s *Server) GetServerStatistics(w http.ResponseWriter, r *http.Request) {
	photos, usage, err := s.st.Totals(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, ServerStatsResponseDto{
		Photos:      photos,
		Usage:       usage,
		UsagePhotos: usage,
		UsageByUser: []UsageByUserDto{{
			UserId: userID, UserName: ownerUserName,
			Photos: photos, Usage: usage,
		}},
	})
}
