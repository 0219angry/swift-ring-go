package ringbuilder

func deepCopy(from []map[string]interface{}) []map[string]interface{} {
	copiedSlice := make([]map[string]interface{}, len(from))
	for i, v := range from {
		copiedElement := make(map[string]interface{})
		for key, value := range v {
			copiedElement[key] = value
		}
		copiedSlice[i] = copiedElement
	}
	return copiedSlice
}
