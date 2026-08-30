// Package gatewayphase serves minimal public-API stubs for devshardctl's
// ChainPhaseGate in the testenv (epochs/latest + current participants).
package gatewayphase

import (
	"fmt"
	"net/http"

	"github.com/labstack/echo/v4"
)

// Config tunes the stub epoch/participant responses.
type Config struct {
	BlockHeight int64
	EpochIndex  uint64
	// PoCStartBlockHeight is latest_epoch.poc_start_block_height.
	PoCStartBlockHeight int64
	// Phase is latest_epoch's epoch phase; empty means Inference, the phase that blocks nothing.
	Phase string
	// ConfirmationPoCActive drives is_confirmation_poc_active, the other half of the blocking state.
	ConfirmationPoCActive bool
	// Participants is what current/participants reports; empty means no weights and everyone preserved.
	Participants []Participant
	// Live, when set, is read per request; without it the values are those of the moment Mount ran.
	Live func() Config
}

// Participant is one entry of the current-participants response; the weight rides on an ML node.
type Participant struct {
	Address      string
	InferenceURL string
	Models       []string
	PoCWeight    uint64
	Preserved    bool
}

func (p Participant) render() map[string]any {
	models := p.Models
	if len(models) == 0 {
		models = []string{"stub-model"}
	}
	nodes := make([]any, 0, len(models))
	for range models {
		nodes = append(nodes, map[string]any{
			"ml_nodes": []any{map[string]any{
				"node_id":             p.Address + "-node",
				"timeslot_allocation": []bool{true, p.Preserved},
				"poc_weight":          fmt.Sprintf("%d", p.PoCWeight),
			}},
		})
	}
	return map[string]any{
		"index":         p.Address,
		"inference_url": p.InferenceURL,
		"models":        models,
		"ml_nodes":      nodes,
	}
}

// phaseOf keeps Inference the default.
func phaseOf(cfg Config) string {
	if cfg.Phase == "" {
		return "Inference"
	}
	return cfg.Phase
}

func withDefaults(cfg Config) Config {
	if cfg.BlockHeight == 0 {
		cfg.BlockHeight = 150
	}
	if cfg.EpochIndex == 0 {
		cfg.EpochIndex = 1
	}
	if cfg.PoCStartBlockHeight == 0 {
		cfg.PoCStartBlockHeight = cfg.BlockHeight - 50
	}
	return cfg
}

// Mount registers GET /v1/epochs/latest and GET /v1/epochs/current/participants.
func Mount(g *echo.Group, mounted Config) {
	if g == nil {
		return
	}
	cfg := withDefaults(mounted)
	g.GET("/v1/epochs/latest", func(c echo.Context) error {
		cfg := cfg
		if mounted.Live != nil {
			cfg = withDefaults(mounted.Live())
		}
		return c.JSON(http.StatusOK, map[string]any{
			"block_height": fmt.Sprintf("%d", cfg.BlockHeight),
			"phase":        phaseOf(cfg),
			"latest_epoch": map[string]any{
				"index":                  fmt.Sprintf("%d", cfg.EpochIndex),
				"poc_start_block_height": fmt.Sprintf("%d", cfg.PoCStartBlockHeight),
			},
			"epoch_stages": map[string]any{
				"epoch_index":        fmt.Sprintf("%d", cfg.EpochIndex),
				"set_new_validators": fmt.Sprintf("%d", cfg.BlockHeight+30),
				"next_poc_start":     fmt.Sprintf("%d", cfg.BlockHeight+50),
			},
			"next_epoch_stages": map[string]any{
				"epoch_index":        fmt.Sprintf("%d", cfg.EpochIndex+1),
				"set_new_validators": fmt.Sprintf("%d", cfg.BlockHeight+450),
			},
			"is_confirmation_poc_active": cfg.ConfirmationPoCActive,
		})
	})
	g.GET("/v1/epochs/current/participants", func(c echo.Context) error {
		current := cfg
		if mounted.Live != nil {
			current = withDefaults(mounted.Live())
		}
		participants := make([]any, 0, len(current.Participants))
		for _, participant := range current.Participants {
			participants = append(participants, participant.render())
		}
		return c.JSON(http.StatusOK, map[string]any{
			"active_participants": map[string]any{
				"participants": participants,
			},
		})
	})
}
