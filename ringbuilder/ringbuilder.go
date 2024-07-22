package ringbuilder

type RingBuilderParameters struct {
	partPower    int
	replicas     int
	minPartHours int
}

type RingBuilder struct {
	RingBuilderParameters
	nextPartPower int
	parts         int
	devs          []map[string]string
	devsChanged   bool
	version       int
	overload      float32
	id            string

	reprica2part2dev [][]string

	lastPartMoves       []byte
	partMovedBitmap     []byte
	lastPartMovesEpoch  int
	lastPartGatherStart int

	dispresionGraph map[string]string
	dispresion      float32

	removeDevs []map[string]string
	ring       []byte
	logger     int
}

func NewRingBuilder(params RingBuilderParameters) *RingBuilder {
	r := new(RingBuilder)

	r.partPower = params.partPower
	r.nextPartPower = 0
	r.replicas = params.replicas
	r.minPartHours = params.minPartHours
	r.parts = 1 << r.partPower
	// r.devs = []map[string]string{}
	r.devsChanged = false
	r.version = 0
	r.overload = 0.0
	r.id = ""

	// r.reprica2part2dev = [][]string{}
	r.lastPartMoves = make([]byte, r.parts)

	return r
}
