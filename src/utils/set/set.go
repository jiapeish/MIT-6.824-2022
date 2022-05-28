package set

type T = interface{}

type Set struct {
	m map[T]struct{}
}

func NewSet(elements ...T) *Set {
	set := NewSetWithSize(len(elements))
	set.Add(elements...)
	return set
}

func NewSetWithSize(size int) *Set {
	return &Set{
		m:     make(map[T]struct{}, size),
	}
}

func (s *Set) Add(elements ...T) {
	for _, element := range elements {
		s.m[element] = struct{}{}
	}
}

func (s *Set) Remove(elements ...T) {
	for _, element := range elements {
		delete(s.m, element)
	}
}

func (s *Set) HasAll(elements ...T) bool {
	for _, element := range elements {
		if _, ok := s.m[element]; !ok {
			return false
		}
	}
	return true
}

func (s *Set) HasAny(elements ...T) bool {
	for _, element := range elements {
		if _, ok := s.m[element]; ok {
			return true
		}
	}
	return false
}

func (s *Set) Len() int {
	return len(s.m)
}

func (s *Set) Clear() {
	s.m = make(map[T]struct{}, 0)
}

func (s *Set) ForEach(f func(element T) bool) {
	for k := range s.m {
		if !f(k) {
			return
		}
	}
}

// To be continued...

