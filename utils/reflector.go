package utils

import (
	"encoding/json"
	"fmt"
	"reflect"
)

func MarshalPublishMsg(msg any) (data []byte) {
	if _, ok := msg.([]byte); ok {
		data = msg.([]byte)
		return data
	}
	var val = reflect.ValueOf(msg)
	val = reflect.Indirect(val)
	switch val.Type().Kind() {
	case reflect.Struct, reflect.Map, reflect.Array, reflect.Slice:
		data, _ = json.Marshal(msg)
		return data
	default:
		s, ok := val.Interface().(fmt.Stringer)
		if ok {
			data, _ = json.Marshal(s.String())
			return data
		}
	}
	data = []byte(fmt.Sprintf("%v", val.Interface()))
	return data
}
