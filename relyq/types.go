package relyq

import (
  "github.com/extemporalgenome/uuid"
)

type ArbitraryTask map[string]interface{}

func (t ArbitraryTask) Id() []byte {
  if id, ok := t["id"]; ok {
    return []byte(id.(string))
  }
  id := uuid.NewRandom().String()
  t["id"] = id
  return []byte(id)
}

type StructuredTask struct {
  RqId []byte `json:"id"`
}

func (t *StructuredTask) Id() []byte {
  if t == nil {
    t = new(StructuredTask)
  }

  if t.RqId == nil {
    t.RqId = uuid.NewRandom().Bytes()
  }
  return t.RqId
}
