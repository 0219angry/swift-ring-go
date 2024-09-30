package ringbuilder

import (
	"log/slog"
	"net"
	"os"
	"sync"
	"time"
)

type RingBuilderParameters struct {
	partPower    int  // number of partitions = 2**partPower
	replicas     int  // number of replicas for each partition
	minPartHours byte // minimum number of hours between partitons change
}

type Device struct {
	// unique interger identifier amongst devices.
	// Default to the next id if the id key is not provided in the dict
	id int
	// a float of the relative weight of this device
	// as compared to others; this indicated how many partitions
	// the builder will try to assign to this device
	weight float64
	// integer indicating which region the device is in
	region int
	// integer indicating which zone the device is in;
	// a given partition will not be assigned to multiple devices
	// within the same (region, zone) pair if there is any alternative
	zone int
	// the ip address of the device
	ip net.IP
	// the tcp port of the device
	port int
	// the device's name on disk (sdb1, for example)
	device string
}
type IndexedDevice struct {
	index  int
	device *Device
}

type RingBuilder struct {
	RingBuilderParameters
	nextPartPower int
	parts         int
	devs          []*Device
	devsChanged   bool
	version       int
	overload      float32
	id            string

	reprica2part2dev [][]string

	lastPartMoves       []byte
	partMovedBitmap     []byte
	lastPartMovesEpoch  time.Time
	lastPartGatherStart int

	dispresionGraph map[string]string
	dispresion      float32

	removeDevs []*Device
	ring       []byte
	logger     *slog.Logger
	logMu      *sync.Mutex
}

const (
	none = -1
)

func NewRingBuilder(params RingBuilderParameters) *RingBuilder {
	r := new(RingBuilder)

	r.partPower = params.partPower
	r.nextPartPower = 0
	r.replicas = params.replicas
	r.minPartHours = params.minPartHours
	r.parts = 1 << r.partPower
	r.devs = make([]*Device, 0)
	r.devsChanged = false
	r.version = 0
	r.overload = 0.0
	r.id = ""

	// r.reprica2part2dev = [][]string{}
	r.lastPartMoves = make([]byte, r.parts)
	r.partMovedBitmap = make([]byte, r.parts/8)
	r.lastPartMovesEpoch = time.Now()

	r.lastPartGatherStart = 0

	// r.dispresionGraph
	r.dispresion = 0.0
	r.removeDevs = make([]*Device, 0)
	// r.ring

	r.logger = slog.New(slog.NewJSONHandler(os.Stdout, nil))
	r.logMu = &sync.Mutex{}

	return r
}

func (r *RingBuilder) SetLogger(l *slog.Logger) {
	if l == nil {
		return
	}
	r.logMu.Lock()
	r.logger = l
	r.logMu.Unlock()
}

func (r *RingBuilder) Id() (string, error) {
	if r.id == "" {
		err := &AttributeError{}
		r.logger.Error(err.Error())
		return r.id, err
	}
	return r.id, nil
}

func (r *RingBuilder) PartShift() int {
	return 32 - r.partPower
}

func (r *RingBuilder) EverRebalanced() bool {
	return false // r.reprica2part2dev があるならtrue
}

func (r *RingBuilder) setPartMoved(part int) {
	r.lastPartMoves[part] = 0
	byte := part / 8
	bit := part % 8
	r.partMovedBitmap[byte] |= (128 >> bit)
}

func (r *RingBuilder) hasPartMoved(part int) bool {
	byte := part / 8
	bit := part % 8
	return (r.partMovedBitmap[byte] & (128 >> bit)) != 0
}

func (r *RingBuilder) canPartMove(part int) bool {
	return (r.lastPartMoves[part] >= r.minPartHours) && !r.hasPartMoved(part)
}

// the total seconds until a rebalance can be performed
func (r *RingBuilder) MinPartSecondsLeft() int {
	elapsedSeconds := time.Since(r.lastPartMovesEpoch).Seconds()
	return max(int(r.minPartHours)*3600-int(elapsedSeconds), 0)
}

// returns the weight of each partition as calculated from
// the total weight of all the devices.
func (r *RingBuilder) WeightOfOnePart() (float64, error) {
	weightSum := 0.0
	for dev := range r.iterDevs() {
		if dev.device.weight > 0 {
			weightSum += dev.device.weight
		} else {
			err := &InvalidWeightError{}
			r.logger.Error(err.Error())
			return 0.0, err
		}
	}

	if weightSum == 0.0 {
		err := &EmptyRingError{}
		r.logger.Error(err.Error())
		return 0.0, err
	}
	return float64(r.parts) * float64(r.replicas) / weightSum, nil
}

func FromDict(builderData *RingBuilder) *RingBuilder {
	params := RingBuilderParameters{1, 1, 1}
	b := NewRingBuilder(params)
	b.CopyFrom(builderData)
	return b
}

// Reinitializes this RingBuilder instance from data obtaind from the builder dict given
// This is to restore a RingBuilder that had its b.toDict() previously saved
func (r *RingBuilder) CopyFrom(builder *RingBuilder) {
	r.partPower = builder.partPower
	r.nextPartPower = builder.nextPartPower
	r.replicas = builder.replicas
	r.minPartHours = builder.minPartHours
	copy(r.devs, builder.devs)
	r.devsChanged = builder.devsChanged
	r.overload = builder.overload
	r.version = builder.version
	copy(r.reprica2part2dev, builder.reprica2part2dev)
	copy(r.lastPartMoves, builder.lastPartMoves)
	r.lastPartMovesEpoch = builder.lastPartMovesEpoch
	copy(r.lastPartMoves, builder.lastPartMoves)
	r.lastPartGatherStart = builder.lastPartGatherStart
	dispresionGraph := make(map[string]string)
	for key, value := range builder.dispresionGraph {
		dispresionGraph[key] = value
	}
	r.dispresionGraph = dispresionGraph
	copy(r.removeDevs, builder.removeDevs)
	r.id = builder.id
	r.ring = nil

}

// :param minPartHours: new value for minPartHours
func (r *RingBuilder) ChangeMinPartHours(minPartHours byte) {
	r.minPartHours = minPartHours
}

// :param newReplicaCount: new value for replicas
func (r *RingBuilder) SetReplicas(newReplicaCount int) {
	oldSlotsUsed := r.parts * r.replicas
	newSlotsUsed := r.parts * newReplicaCount
	if oldSlotsUsed != newSlotsUsed {
		r.devsChanged = true
	}
	r.replicas = newReplicaCount
}

func (r *RingBuilder) SetOverload(overload float32) {
	r.overload = overload
}

func (r *RingBuilder) GetRing() {

}

func (r *RingBuilder) AddDev(dev *Device) (int, error) {
	// check missing device information
	missing := make([]string, 0)

	if dev.region == 0 {
		missing = append(missing, "region")
	}
	if dev.zone == 0 {
		missing = append(missing, "zone")
	}
	if dev.ip == nil {
		missing = append(missing, "ip")
	}
	if dev.port == 0 {
		missing = append(missing, "port")
	}
	if dev.device == "" {
		missing = append(missing, "device")
	}
	if dev.weight == 0 {
		missing = append(missing, "weight")
	}

	if len(missing) > 0 {
		err := &ValueError{ID: dev.id, Missing: missing}
		r.logger.Error(err.Error())
		return dev.id, err
	}

	if dev.id == none {
		if len(r.devs) > 0 {
			found := false
			for v := range r.iterDevs() {
				if v.device == nil {
					dev.id = v.index
					r.devs[v.index] = dev
					found = true
					break
				}
			}
			if !found {
				dev.id = len(r.devs)
				r.devs = append(r.devs, dev)
			}
		} else {
			dev.id = 0
			r.devs = append(r.devs, dev)
		}
	} else {
		// Check for duplicate device ids in r.devs
		if dev.id < len(r.devs) && r.devs[dev.id] != nil {
			err := &DuplicateDeviceError{DupulicateDeviceID: dev.id}
			r.logger.Error(err.Error())
			return none, err
		}
	}

	// Add holes to r.devs to ensure r.devs[dev.id] will be the dev
	for dev.id >= len(r.devs) {
		r.devs = append(r.devs, nil)
	}

	r.devs[dev.id] = dev
	r.devsChanged = true
	r.version += 1

	return dev.id, nil
}

/*
Set the weight of a device. This should be called rather than
just altering the weight key in the device dict directly,
as the builder will need to rebuild some internal state
to reflect the change.

:param devID: device id
:param weight: new weight for device
*/
func (r *RingBuilder) SetDevWeight(devID int, weight float64) error {
	if _, exist := devIsExistIn(r.removeDevs, devID); exist {
		err := &RemovedDeviceError{ID: devID, IncompletedOperation: "SetDevWeight"}
		r.logger.Error(err.Error())
		return err
	}
	r.devs[devID].weight = weight
	r.devsChanged = true
	r.version += 1
	return nil
}

/*
Set the region of a device. This shoud be called rather than
just altering the region key in the device dict directly,
as the builder will need to rebuild some internal state
to reflect the change.

:param dev_id: device id
:param region: new region for device
*/
func (r *RingBuilder) SetDevRegion(devID int, region int) error {
	if _, exist := devIsExistIn(r.removeDevs, devID); exist {
		err := &RemovedDeviceError{ID: devID, IncompletedOperation: "SetDevRegion"}
		r.logger.Error(err.Error())
		return err
	}
	r.devs[devID].region = region
	r.devsChanged = true
	r.version += 1
	return nil
}

/*
Set the zone of a device. This shoud be called rather than
just altering the region key in the device dict directly,
as the builder will need to rebuild some internal state
to reflect the change.

:param devID: device id
:param zone: new zone for device
*/
func (r *RingBuilder) SetDevZone(devID int, zone int) error {
	if _, exist := devIsExistIn(r.removeDevs, devID); exist {
		err := &RemovedDeviceError{ID: devID, IncompletedOperation: "SetDevRegion"}
		r.logger.Error(err.Error())
		return err
	}
	r.devs[devID].zone = zone
	r.devsChanged = true
	r.version += 1
	return nil
}

/*
Remove a device from the ring.

:param devID: device id
*/
func (r *RingBuilder) Remove_dev(devID int) error {
	if _, exist := devIsExistIn(r.removeDevs, devID); exist {
		err := &RemovedDeviceError{ID: devID, IncompletedOperation: "RemoveDev"}
		r.logger.Error(err.Error())
		return err
	}
	dev := r.devs[devID]
	dev.weight = 0
	r.removeDevs = append(r.removeDevs, dev)
	r.devsChanged = true
	r.version += 1
	return nil
}

/*
Rebalance the ring.

	This is the main work function of the builder, as it will assign and
	reassign partitions to devices in the ring based on weights, distinct
	zones, recent reassignments, etc.

	The process doesn't always perfectly assign partitions (that'd take a
	lot more analysis and therefore a lot more time -- I had code that did
	that before). Because of this, it keeps rebalancing until the device
	skew (number of partitions a device wants compared to what it has) gets
	below 1% or doesn't change by more than 1% (only happens with a ring
	that can't be balanced no matter what).
*/
func (r *RingBuilder) Rebalance(seed int) {

}

func (r *RingBuilder) iterDevs() (c chan *IndexedDevice) {
	c = make(chan *IndexedDevice, 1)
	go func() {
		for i, dev := range r.devs {
			if dev.id != none {
				c <- &IndexedDevice{index: i, device: dev}
			}
		}
		close(c)
	}()

	return
}
