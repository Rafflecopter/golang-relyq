// Package marshallers provides objects for marshalling tasks into bytes
package marshallers

import (
	"encoding/json"
)

var (
	Json JsonMarshaller = JsonMarshaller(true)
)

type Marshaller interface {
	Marshal(interface{}) ([]byte, error)
	Unmarshal([]byte, interface{}) error
}

type JsonMarshaller bool

func (z JsonMarshaller) Marshal(obj interface{}) ([]byte, error) {
	return json.Marshal(obj)
}
func (z JsonMarshaller) Unmarshal(enc []byte, obj interface{}) error {
	err := json.Unmarshal(enc, obj)
	return err
}
