package tarbase

import (
	"testing"
)

func TestClearInvalid(t *testing.T){
	if strs := ClearInvalid("D://///123.txt");strs != "D://123.txt" {
		t.Errorf("func is error : %s",strs)
	}
}