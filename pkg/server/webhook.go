package server

import (
	"cmp"
	json "github.com/bytedance/sonic"
	"net/http"
)

func (s *Server) handleTautulli(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse the JSON body from Tautulli
	var payload struct {
		Type        string `json:"type"`
		TvdbID      string `json:"tvdb_id"`
		TmdbID      string `json:"tmdb_id"`
		Topic       string `json:"topic"`
		AutoProcess bool   `json:"autoProcess"`
	}

	if err := json.ConfigDefault.NewDecoder(r.Body).Decode(&payload); err != nil {
		s.logger.Error().Err(err).Msg("Failed to parse webhook body")
		http.Error(w, "Failed to parse webhook body: "+err.Error(), http.StatusBadRequest)
		return
	}

	if payload.Topic != "tautulli" {
		http.Error(w, "Invalid topic", http.StatusBadRequest)
		return
	}

	if payload.TmdbID == "" && payload.TvdbID == "" {
		http.Error(w, "Invalid ID", http.StatusBadRequest)
		return
	}

	mediaId := cmp.Or(payload.TmdbID, payload.TvdbID)
	if _, err := s.manager.Repair().AddJob([]string{}, []string{mediaId}, payload.AutoProcess, false); err != nil {
		http.Error(w, "Failed to add job: "+err.Error(), http.StatusInternalServerError)
		return
	}
}
