package ringbuilder

type AttributeError struct{}

func (e *AttributeError) Error() string {
	errmsg := "id attribute has not bee initialized by calling save()"
	return errmsg
}

type EmptyRingError struct{}

func (e *EmptyRingError) Error() string {
	errmsg := "There are no device in this ring, or all devices have been deleted"
	return errmsg
}

type InvalidWeightError struct{}

func (e *InvalidWeightError) Error() string {
	errmsg := "Invalid weight type for device"
	return errmsg
}
