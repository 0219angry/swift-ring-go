package ringbuilder

import (
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewRingBuileder(t *testing.T) {
	params := RingBuilderParameters{
		partPower:    10,
		replicas:     3,
		minPartHours: 1,
	}

	rb := NewRingBuilder(params)

	assert.Equal(t, params.partPower, rb.partPower)
	assert.Equal(t, params.replicas, rb.replicas)
	assert.Equal(t, params.minPartHours, rb.minPartHours)
	assert.Equal(t, 1<<params.partPower, rb.parts)
	assert.NotNil(t, rb.logger)
}

func TestSetLogger(t *testing.T) {
	params := RingBuilderParameters{
		partPower:    10,
		replicas:     3,
		minPartHours: 1,
	}
	rb := NewRingBuilder(params)

	anothorLogger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	rb.SetLogger(anothorLogger)

	assert.Equal(t, anothorLogger, rb.logger)
}
