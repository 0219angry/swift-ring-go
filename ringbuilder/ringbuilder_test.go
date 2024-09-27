package ringbuilder

import (
	"log/slog"
	"net"
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

func TestRingBuilder_SetLogger(t *testing.T) {
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

// func TestId(t *testing.T) {

// }

func TestRingBuilder_AddDev(t *testing.T) {
	t.Run("正常系01 1件のデバイスを追加", func(t *testing.T) {
		params := RingBuilderParameters{
			partPower:    10,
			replicas:     3,
			minPartHours: 1,
		}
		rb := NewRingBuilder(params)

		newDevice := &Device{
			id:     none,
			weight: 1.0,
			region: 1,
			zone:   1,
			ip:     net.IPv4(192, 168, 0, 1),
			port:   8080,
			device: "sdb1",
		}

		addedDeviceId, _ := rb.AddDev(newDevice)

		assert.Equal(t, 0, rb.devs[0].id)
		assert.Equal(t, 0, addedDeviceId)
		assert.Equal(t, newDevice.weight, rb.devs[0].weight)
		assert.Equal(t, newDevice.region, rb.devs[0].region)
		assert.Equal(t, newDevice.zone, rb.devs[0].zone)
		assert.Equal(t, newDevice.ip, rb.devs[0].ip)
		assert.Equal(t, newDevice.port, rb.devs[0].port)
		assert.Equal(t, newDevice.device, rb.devs[0].device)
	})

}
