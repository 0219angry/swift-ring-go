package ringbuilder

import (
	"fmt"
	"strings"
)

type AttributeError struct{}

func (e *AttributeError) Error() string {
	errmsg := "id attribute has not bee initialized by calling save()"
	return errmsg
}

type EmptyRingError struct{}

func (e *EmptyRingError) Error() string {
	errmsg := "there are no device in this ring, or all devices have been deleted"
	return errmsg
}

type InvalidWeightError struct{}

func (e *InvalidWeightError) Error() string {
	errmsg := "invalid weight type for device"
	return errmsg
}

type DuplicateDeviceError struct {
	dupulicateDeviceID int
}

func (e *DuplicateDeviceError) Error() string {
	errmsg := fmt.Sprintf("duplicate device id: %d", e.dupulicateDeviceID)
	return errmsg
}

type ValueError struct {
	id      int
	missing []string
}

func (e *ValueError) Error() string {
	missingString := strings.Join(e.missing, ",")
	errmsg := fmt.Sprintf("device %d is missing required key(s): %s", e.id, missingString)
	return errmsg
}
