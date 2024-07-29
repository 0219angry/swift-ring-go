package ringbuilder

import (
	"log/slog"
	"os"
	"sync"
	"time"
)

type RingBuilderParameters struct {
	partPower    int
	replicas     int
	minPartHours byte
}

type RingBuilder struct {
	RingBuilderParameters
	nextPartPower int
	parts         int
	devs          []map[string]interface{}
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

	removeDevs []map[string]interface{}
	ring       []byte
	logger     *slog.Logger
	logMu      *sync.Mutex
}

func NewRingBuilder(params RingBuilderParameters) *RingBuilder {
	r := new(RingBuilder)

	r.partPower = params.partPower
	r.nextPartPower = 0
	r.replicas = params.replicas
	r.minPartHours = params.minPartHours
	r.parts = 1 << r.partPower
	r.devs = make([]map[string]interface{}, 0)
	r.devsChanged = false
	r.version = 0
	r.overload = 0.0
	r.id = ""

	// r.reprica2part2dev = [][]string{}
	r.lastPartMoves = make([]byte, r.parts)
	// r.partMovedBitmap
	r.lastPartMovesEpoch = time.Now()

	r.lastPartGatherStart = 0

	// r.dispresionGraph
	r.dispresion = 0.0
	// r.removeDevs
	// r.ring

	r.logger = slog.New(slog.NewJSONHandler(os.Stdout, nil))

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
	elapsedSeconds := time.Now().Sub(r.lastPartMovesEpoch).Seconds()
	return max(int(r.minPartHours)*3600-int(elapsedSeconds), 0)
}

// returns the weight of each partition as calculated from
// the total weight of all the devices.
func (r *RingBuilder) WeightOfOnePart() (float64, error) {
	weightSum := 0.0
	for dev := range r.iterDevs() {
		if weight, ok := dev["weight"].(float64); ok {
			weightSum += weight
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
	r.devs = deepCopy(builder.devs)
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
	r.removeDevs = deepCopy(builder.removeDevs)
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

func (r *RingBuilder) setOverload(overload float32) {
	r.overload = overload
}

func (r *RingBuilder) getRing() {

}

func (r *RingBuilder) iterDevs() (c chan map[string]interface{}) {
	c = make(chan map[string]interface{}, 1)
	go func() {
		for _, dev := range r.devs {
			if dev["id"] != 0 {
				c <- dev
			}
		}
		close(c)
	}()

	return
}
