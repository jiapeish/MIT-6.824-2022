package raft

import (
	"fmt"
	"strings"
)

type raftLog struct {
	Entries []Entry
	Index0  int
}

func newLog() *raftLog {
	return &raftLog{
		Entries: make([]Entry, 0),
		Index0:  0,
	}
}

func (l *raftLog) append(ents ...Entry) {
	l.Entries = append(l.Entries, ents...)
}

func (l *raftLog) at(index int) *Entry {
	return &l.Entries[index]
}

func (l *raftLog) len() int {
	return len(l.Entries)
}

func (l *raftLog) lastLog() *Entry {
	return l.at(l.len() - 1)
}

func (l *raftLog) slice(index int) []Entry {
	return l.Entries[index:]
}

func (l *raftLog) truncate(index int) {
	l.Entries = l.Entries[:index]
}

func (l *raftLog) String() string {
	var terms []string
	for _, e := range l.Entries {
		terms = append(terms, fmt.Sprintf("%4d", e.Term))
	}
	return fmt.Sprintf(strings.Join(terms, "|"))
}

type Entry struct {
	Term    int
	Index   int
	Command interface{}
}

func (e *Entry) String() string {
	return fmt.Sprintf("(T:%d|I:%d|Cmd:%v)", e.Term, e.Index, e.Command)
}
