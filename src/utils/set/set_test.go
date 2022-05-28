package set

import "testing"

func TestSet(t *testing.T) {
	num1, num2 := 1, 2
	set := NewSet(num1)
	set.Add(num2)
	if set.Len() != 2 {
		t.Fatalf("[Set] failed, add numbers failed")
	}

	str1, str2 := "hello", "world"
	set.Add(str1)
	set.Add(str2)
	if set.Len() != 4 {
		t.Fatalf("[Set] failed, add strings failed")
	}

	set.Clear()
	if set.Len() != 0 {
		t.Fatalf("[Set] failed, clear failed")
	}

	type composite struct {
		num float64
		flag bool
	}
	comp1 := composite{
		num:  1.23,
		flag: false,
	}
	comp2 := composite{
		num:  -1.23,
		flag: true,
	}
	set.Add(comp1)
	set.Add(comp2)
	if !set.HasAll(comp1, comp2) {
		t.Fatalf("[Set] failed, add composite type failed")
	}

	if !set.HasAny(num1, str1, comp1) {
		t.Fatalf("[Set] failed, check has any failed")
	}
}

