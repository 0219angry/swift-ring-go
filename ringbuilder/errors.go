package ringbuilder

import (
	"fmt"
	"strings"
)

type AttributeError struct {
	name   string
	errmsg string
}

func (e *AttributeError) Error() string {
	e.name = "AttributeError"
	e.errmsg = "id attribute has not bee initialized by calling save()"
	return e.errmsg
}

type EmptyRingError struct {
	name   string
	errmsg string
}

func (e *EmptyRingError) Error() string {
	e.name = "EmptyRingError"
	e.errmsg = "there are no device in this ring, or all devices have been deleted"
	return e.errmsg
}

type InvalidWeightError struct {
	errmsg string
}

func (e *InvalidWeightError) Error() string {
	e.errmsg = "invalid weight type for device"
	return e.errmsg
}

type DuplicateDeviceError struct {
	DupulicateDeviceID int
	errmsg             string
}

func (e *DuplicateDeviceError) Error() string {
	e.errmsg = fmt.Sprintf("duplicate device id: %d", e.DupulicateDeviceID)
	return e.errmsg
}

type ValueError struct {
	ID      int
	Missing []string
	errmsg  string
}

func (e *ValueError) Error() string {
	missingString := strings.Join(e.Missing, ",")
	e.errmsg = fmt.Sprintf("device %d is missing required key(s): %s", e.ID, missingString)
	return e.errmsg
}

type RemovedDeviceError struct {
	ID                   int
	IncompletedOperation string
	errmsg               string
}

func (e *RemovedDeviceError) Error() string {
	e.errmsg = fmt.Sprintf("operation could not be completed on devID %d because it is marked for removal (operation: %s)", e.ID, e.IncompletedOperation)
	return e.errmsg
}
