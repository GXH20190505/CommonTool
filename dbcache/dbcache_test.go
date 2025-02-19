package dbcache

import (
	"models"
	"testing"
)

func Test_parseTag(t *testing.T) {
	js := new(models.TD_DEVICE_STATE)
	mp := parseTag(js)
	t.Log("js tag", mp)

}

func Test_parseVal(t *testing.T) {
	js := new(models.TD_DEVICE_STATE)
	js.DEVICE_ID = "DID"
	js.FREZZ_STATE = "F"
	js.LAST_TIME = "L"
	js.ONLINE_STATE = "S"
	js.UPDATE_TIME = "T"

	mp := parseVal(js)
	t.Log("js val", mp)
}
