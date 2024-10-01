package ringbuilder

import (
	"log/slog"
	"net"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewRingBuileder(t *testing.T) {
	t.Run("正常系01 パラメータを指定してRingBuilderを生成", func(t *testing.T) {

		params := RingBuilderParameters{
			partPower:    10,
			replicas:     3,
			minPartHours: 1,
		}

		rb, err := NewRingBuilder(params)

		assert.NoError(t, err)
		assert.Equal(t, params.partPower, rb.partPower)
		assert.Equal(t, params.replicas, rb.replicas)
		assert.Equal(t, params.minPartHours, rb.minPartHours)
		assert.Equal(t, 1<<params.partPower, rb.parts)
		assert.Equal(t, []*Device{}, rb.devs)
		assert.False(t, rb.devsChanged)
		assert.Equal(t, 0, rb.version)
		assert.Equal(t, 0.0, rb.overload)
		assert.Equal(t, "", rb.id)
		assert.Equal(t, 1<<params.partPower, len(rb.lastPartMoves))
		assert.Equal(t, 1<<params.partPower/8, len(rb.partMovedBitmap))
		assert.Equal(t, 0, rb.lastPartGatherStart)
		assert.Equal(t, 0.0, rb.dispresion)
		assert.Equal(t, 0, len(rb.removeDevs))
		assert.NotNil(t, rb.logger)
	})
	t.Run("異常系01 PartPowerが-1", func(t *testing.T) {
		rb, err := NewRingBuilder(RingBuilderParameters{partPower: -1, replicas: 3, minPartHours: 1})

		assert.Nil(t, rb)
		assert.Error(t, err)
		assert.IsType(t, &ParameterValueError{}, err)
	})
	t.Run("異常系02 PartPowerが33", func(t *testing.T) {
		rb, err := NewRingBuilder(RingBuilderParameters{partPower: 33, replicas: 3, minPartHours: 1})

		assert.Nil(t, rb)
		assert.Error(t, err)
		assert.IsType(t, &ParameterValueError{}, err)
	})
	t.Run("異常系03 replicasが0", func(t *testing.T) {
		rb, err := NewRingBuilder(RingBuilderParameters{partPower: 10, replicas: 0, minPartHours: 1})

		assert.Nil(t, rb)
		assert.Error(t, err)
		assert.IsType(t, &ParameterValueError{}, err)
	})
	t.Run("異常系04 minPartHoursが0", func(t *testing.T) {
		rb, err := NewRingBuilder(RingBuilderParameters{partPower: 10, replicas: 3, minPartHours: 0})

		assert.Nil(t, rb)
		assert.Error(t, err)
		assert.IsType(t, &ParameterValueError{}, err)
	})
}

func TestRingBuilder_SetLogger(t *testing.T) {
	t.Run("正常系01 ロガーを設定", func(t *testing.T) {
		params := RingBuilderParameters{
			partPower:    10,
			replicas:     3,
			minPartHours: 1,
		}
		rb, err := NewRingBuilder(params)
		version := rb.version

		assert.NoError(t, err)

		anothorLogger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
		rb.SetLogger(anothorLogger)

		assert.Equal(t, anothorLogger, rb.logger)
		assert.Equal(t, version, rb.version) // loggerが変わってもversionは変わらない
	})
}

// func TestId(t *testing.T) {

// }
func TestRingBuilder_PartShift(t *testing.T) {
	t.Run("正常系01 シフト量を計算", func(t *testing.T) {
		rb, err := NewRingBuilder(RingBuilderParameters{partPower: 10, replicas: 3, minPartHours: 1})
		assert.NoError(t, err)
		assert.Equal(t, 32-10, rb.PartShift())
	})
}

func TestRingBuilder_AddDev(t *testing.T) {
	t.Run("正常系01 1件のデバイスを追加", func(t *testing.T) {
		params := RingBuilderParameters{
			partPower:    10,
			replicas:     3,
			minPartHours: 1,
		}
		rb, err := NewRingBuilder(params)
		version := rb.version

		assert.NoError(t, err)

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
		assert.Equal(t, version+1, rb.version)
	})

	t.Run("正常系02 複数のデバイスを追加", func(t *testing.T) {
		rb, err := NewRingBuilder(RingBuilderParameters{partPower: 10, replicas: 3, minPartHours: 1})

		assert.NoError(t, err)

		devices := []*Device{
			{id: none, weight: 1.0, region: 1, zone: 1, ip: net.IPv4(192, 168, 0, 1), port: 8080, device: "sdb1"},
			{id: none, weight: 2.0, region: 1, zone: 2, ip: net.IPv4(192, 168, 0, 2), port: 8080, device: "sdb2"},
			{id: none, weight: 3.0, region: 2, zone: 1, ip: net.IPv4(192, 168, 0, 3), port: 8080, device: "sdb3"},
		}

		for i, dev := range devices {
			id, err := rb.AddDev(dev)
			assert.NoError(t, err)
			assert.Equal(t, i, id)
		}

		assert.Equal(t, 3, len(rb.devs))
		assert.True(t, rb.devsChanged)
		assert.Equal(t, 3, rb.version)
	})

	t.Run("正常系03 デバイスIDの自動割り当て", func(t *testing.T) {
		rb, err := NewRingBuilder(RingBuilderParameters{partPower: 10, replicas: 3, minPartHours: 1})
		version := rb.version
		assert.NoError(t, err)

		device1 := &Device{id: none, weight: 1.0, region: 1, zone: 1, ip: net.IPv4(192, 168, 0, 1), port: 8080, device: "sdb1"}
		device2 := &Device{id: none, weight: 2.0, region: 1, zone: 2, ip: net.IPv4(192, 168, 0, 2), port: 8080, device: "sdb2"}

		id1, _ := rb.AddDev(device1)
		id2, _ := rb.AddDev(device2)

		assert.Equal(t, 0, id1)
		assert.Equal(t, 1, id2)
		assert.Equal(t, 2, len(rb.devs))
		assert.Equal(t, version+2, rb.version)
	})

	t.Run("異常系01 不完全なデバイス情報", func(t *testing.T) {
		rb, err := NewRingBuilder(RingBuilderParameters{partPower: 10, replicas: 3, minPartHours: 1})
		version := rb.version
		assert.NoError(t, err)

		incompleteDevice := &Device{
			id:     none,
			weight: 1.0,
			region: 1,
			// zoneが欠落
			ip:     net.IPv4(192, 168, 0, 1),
			port:   8080,
			device: "sdb1",
		}

		id, err := rb.AddDev(incompleteDevice)
		assert.Equal(t, none, id)
		assert.Error(t, err)
		assert.IsType(t, &ValueError{}, err)
		assert.Equal(t, version, rb.version)
		assert.False(t, rb.devsChanged)
	})

	t.Run("異常系02 重複するデバイスID", func(t *testing.T) {
		rb, err := NewRingBuilder(RingBuilderParameters{partPower: 10, replicas: 3, minPartHours: 1})
		version := rb.version
		device1 := &Device{id: 0, weight: 1.0, region: 1, zone: 1, ip: net.IPv4(192, 168, 0, 1), port: 8080, device: "sdb1"}
		device2 := &Device{id: 0, weight: 2.0, region: 1, zone: 2, ip: net.IPv4(192, 168, 0, 2), port: 8080, device: "sdb2"}

		idDevice1, err := rb.AddDev(device1)
		assert.Equal(t, 0, idDevice1)
		assert.NoError(t, err)
		rb.setDevsChanged(false)

		idDevice2, err := rb.AddDev(device2)
		assert.Equal(t, none, idDevice2)
		assert.Error(t, err)
		assert.IsType(t, &DuplicateDeviceError{}, err)
		assert.Equal(t, version+1, rb.version) // 1件追加されただけ
		assert.False(t, rb.devsChanged)
	})

}

func TestRingBuilder_SetDevWeight(t *testing.T) {
	t.Run("正常系01 デバイスの重みを設定", func(t *testing.T) {
		rb, err := NewRingBuilder(RingBuilderParameters{partPower: 10, replicas: 3, minPartHours: 1})
		version := rb.version
		assert.NoError(t, err)

		device := &Device{id: 0, weight: 1.0, region: 1, zone: 1, ip: net.IPv4(192, 168, 0, 1), port: 8080, device: "sdb1"}
		rb.AddDev(device)
		rb.setDevsChanged(false)

		newWeight := 2.0
		err = rb.SetDevWeight(device.id, newWeight)

		assert.NoError(t, err)

		assert.Equal(t, newWeight, rb.devs[0].weight)
		assert.Equal(t, version+2, rb.version)
	})
	t.Run("異常系01 重みが0未満", func(t *testing.T) {
		rb, err := NewRingBuilder(RingBuilderParameters{partPower: 10, replicas: 3, minPartHours: 1})
		version := rb.version

		assert.NoError(t, err)

		device := &Device{id: 0, weight: 1.0, region: 1, zone: 1, ip: net.IPv4(192, 168, 0, 1), port: 8080, device: "sdb1"}
		rb.AddDev(device)
		rb.setDevsChanged(false)
		newWeight := -1.0
		err = rb.SetDevWeight(device.id, newWeight)
		assert.Error(t, err)
		assert.IsType(t, &ParameterValueError{}, err)
		assert.False(t, rb.devsChanged)
		assert.Equal(t, version+1, rb.version)
	})
}

func TestRingBuilder_SetDevRegion(t *testing.T) {
	t.Run("正常系01 デバイスのリージョンを設定", func(t *testing.T) {
		rb, err := NewRingBuilder(RingBuilderParameters{partPower: 10, replicas: 3, minPartHours: 1})
		version := rb.version
		assert.NoError(t, err)

		device := &Device{id: 0, weight: 1.0, region: 1, zone: 1, ip: net.IPv4(192, 168, 0, 1), port: 8080, device: "sdb1"}
		rb.AddDev(device)
		rb.setDevsChanged(false)
		newRegion := 2
		rb.SetDevRegion(device.id, newRegion)

		assert.Equal(t, newRegion, rb.devs[0].region)
		assert.True(t, rb.devsChanged)
		assert.Equal(t, version+2, rb.version)
	})
	t.Run("異常系01 リージョンが0以下", func(t *testing.T) {
		rb, err := NewRingBuilder(RingBuilderParameters{partPower: 10, replicas: 3, minPartHours: 1})
		version := rb.version

		assert.NoError(t, err)

		device := &Device{id: 0, weight: 1.0, region: 1, zone: 1, ip: net.IPv4(192, 168, 0, 1), port: 8080, device: "sdb1"}
		rb.AddDev(device)
		rb.setDevsChanged(false)

		newRegion := 0
		err = rb.SetDevRegion(device.id, newRegion)
		assert.Error(t, err)
		assert.IsType(t, &ParameterValueError{}, err)
		assert.Equal(t, version+1, rb.version)
		assert.False(t, rb.devsChanged)
	})
}

func TestRingBuilder_SetDevZone(t *testing.T) {
	t.Run("正常系01 デバイスのゾーンを設定", func(t *testing.T) {
		rb, err := NewRingBuilder(RingBuilderParameters{partPower: 10, replicas: 3, minPartHours: 1})
		version := rb.version

		assert.NoError(t, err)

		device := &Device{id: 0, weight: 1.0, region: 1, zone: 1, ip: net.IPv4(192, 168, 0, 1), port: 8080, device: "sdb1"}
		rb.AddDev(device)
		rb.setDevsChanged(false)
		newZone := 2
		rb.SetDevZone(device.id, newZone)

		assert.Equal(t, newZone, rb.devs[0].zone)
		assert.Equal(t, version+2, rb.version)
		assert.True(t, rb.devsChanged)
	})
	t.Run("異常系01 ゾーンが0以下", func(t *testing.T) {
		rb, err := NewRingBuilder(RingBuilderParameters{partPower: 10, replicas: 3, minPartHours: 1})
		version := rb.version
		assert.NoError(t, err)

		device := &Device{id: 0, weight: 1.0, region: 1, zone: 1, ip: net.IPv4(192, 168, 0, 1), port: 8080, device: "sdb1"}
		rb.AddDev(device)
		rb.setDevsChanged(false)
		newZone := 0
		err = rb.SetDevZone(device.id, newZone)
		assert.Error(t, err)
		assert.IsType(t, &ParameterValueError{}, err)
		assert.Equal(t, version+1, rb.version)
		assert.False(t, rb.devsChanged)
	})
}

func TestRingBuilder_RemoveDev(t *testing.T) {
	t.Run("正常系01 デバイスを削除", func(t *testing.T) {
		rb, err := NewRingBuilder(RingBuilderParameters{partPower: 10, replicas: 3, minPartHours: 1})
		version := rb.version
		assert.NoError(t, err)

		device := &Device{id: 0, weight: 1.0, region: 1, zone: 1, ip: net.IPv4(192, 168, 0, 1), port: 8080, device: "sdb1"}
		rb.AddDev(device)
		rb.setDevsChanged(false)

		rb.RemoveDev(device.id)

		assert.Equal(t, 0.0, rb.devs[0].weight)
		assert.Equal(t, 1, len(rb.removeDevs))
		assert.Equal(t, version+2, rb.version)
		assert.True(t, rb.devsChanged)
	})

	t.Run("異常系01 削除されたデバイスを再度削除", func(t *testing.T) {
		rb, err := NewRingBuilder(RingBuilderParameters{partPower: 10, replicas: 3, minPartHours: 1})
		version := rb.version
		assert.NoError(t, err)

		device := &Device{id: 0, weight: 1.0, region: 1, zone: 1, ip: net.IPv4(192, 168, 0, 1), port: 8080, device: "sdb1"}
		rb.AddDev(device)
		rb.setDevsChanged(false)
		rb.RemoveDev(device.id)
		rb.setDevsChanged(false)

		err = rb.RemoveDev(device.id)
		assert.Error(t, err)
		assert.IsType(t, &RemovedDeviceError{}, err)
		assert.Equal(t, version+2, rb.version)
		assert.False(t, rb.devsChanged)
	})

	t.Run("異常系02 存在しないデバイスを削除", func(t *testing.T) {
		rb, err := NewRingBuilder(RingBuilderParameters{partPower: 10, replicas: 3, minPartHours: 1})
		version := rb.version

		assert.NoError(t, err)

		device := &Device{id: 0, weight: 1.0, region: 1, zone: 1, ip: net.IPv4(192, 168, 0, 1), port: 8080, device: "sdb1"}
		rb.AddDev(device)

		rb.setDevsChanged(false)
		err = rb.RemoveDev(device.id + 1)
		assert.Error(t, err)
		assert.IsType(t, &UnknownDeviceError{}, err)
		assert.Equal(t, version+1, rb.version)
		assert.False(t, rb.devsChanged)
	})
}
