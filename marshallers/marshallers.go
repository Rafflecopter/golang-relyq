// Package marshallers provides objects for marshalling tasks into bytes
package marshallers

import (
	"encoding/json"
)

var (
	JSON JSONMarshaller = JSONMarshaller(true)
)

type Marshaller interface {
	Marshal(map[string]interface{}) ([]byte, error)
	Unmarshal([]byte) (map[string]interface{}, error)
}

type JSONMarshaller bool

func (z JSONMarshaller) Marshal(obj map[string]interface{}) ([]byte, error) {
	return json.Marshal(obj)
}
func (z JSONMarshaller) Unmarshal(enc []byte) (map[string]interface{}, error) {
	obj := make(map[string]interface{})
	err := json.Unmarshal(enc, &obj)
	return obj, err
}
