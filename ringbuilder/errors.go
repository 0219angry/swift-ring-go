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
	name   string
	errmsg string
}

func (e *InvalidWeightError) Error() string {
	e.name = "InvalidWeightError"
	e.errmsg = "invalid weight type for device"
	return e.errmsg
}

type DuplicateDeviceError struct {
	name               string
	DupulicateDeviceID int
	errmsg             string
}

func (e *DuplicateDeviceError) Error() string {
	e.name = "DuplicateDeviceError"
	e.errmsg = fmt.Sprintf("duplicate device id: %d", e.DupulicateDeviceID)
	return e.errmsg
}

type ValueError struct {
	name   string
	ID     int
	Missing []string
	errmsg string
}

func (e *ValueError) Error() string {
	e.name = "ValueError"
	missingString := strings.Join(e.Missing, ",")
	e.errmsg = fmt.Sprintf("device %d is missing required key(s): %s", e.ID, missingString)
	return e.errmsg
}

type RemovedDeviceError struct {
	name                 string
	ID                   int
	IncompletedOperation string
	errmsg               string
}

func (e *RemovedDeviceError) Error() string {
	e.name = "RemovedDeviceError"
	e.errmsg = fmt.Sprintf("operation could not be completed on devID %d because it is marked for removal (operation: %s)", e.ID, e.IncompletedOperation)
	return e.errmsg
}

type UnknownDeviceError struct {
	name string
	ID   int
	errmsg string
}

func (e *UnknownDeviceError) Error() string {
	e.name = "UnknownDeviceError"
	e.errmsg = fmt.Sprintf("device %d is not in the ring", e.ID)
	return e.errmsg
}

type ParameterValueError struct {
	name      string
	Parameter string
	Details   string
	errmsg    string
}

func (e *ParameterValueError) Error() string {
	e.name = "ParameterValueError"
	e.errmsg = fmt.Sprintf("parameter %s is invalid.(%s)", e.Parameter, e.Details)
	return e.errmsg
}
