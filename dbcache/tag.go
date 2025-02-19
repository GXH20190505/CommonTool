package dbcache

import (
	"reflect"
)

/** json字段处理
 */
func parseTag(js interface{}) []string {
	var tagList []string

	t := reflect.TypeOf(js)
	v := reflect.ValueOf(js)

	if t.Kind() == reflect.Map {
		for k, _ := range v.Interface().(map[string]string) {
			tagList = append(tagList, k)
		}
	} else if t.Kind() == reflect.Ptr {
		for i := 0; i < t.Elem().NumField(); i++ {
			fields := t.Elem().Field(i)
			tag := fields.Tag.Get("bson")
			if tag == "" || tag == "-" {
				continue
			}
			tagList = append(tagList, tag)
		}
	}

	return tagList
}

/** json数据处理
 */
func parseVal(js interface{}) map[string]string {

	mp := make(map[string]string)
	t := reflect.TypeOf(js)
	v := reflect.ValueOf(js)

	switch v.Kind() {
	case reflect.Ptr: //json
	case reflect.Map:
		return v.Interface().(map[string]string)
	default:
		return mp
	}
	for i := 0; i < t.Elem().NumField(); i++ {
		tag := t.Elem().Field(i).Tag.Get("bson")
		val := v.Elem().Field(i).String()
		mp[tag] = val
	}
	return mp
}
