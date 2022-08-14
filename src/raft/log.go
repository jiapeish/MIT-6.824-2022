package raft

import (
	"fmt"
	"strings"
)

type raftLog struct {
	Entries []Entry

	// for 2D, when you're gonna cut up beginning of the log, this global log,
	// You need to keep track of the index of the first entry recorded in
	// the log.
	//
	// for example, you take a snapshot at index 10, you're gonna cut 0-9,
	// and index0 will be 10. So this means the end of the snapshot, not in
	// the log anymore.
	//
	// in 2A, 2B, 2C, index0 will always be 0.
	Index0 int
}

func newLog() *raftLog {
	return &raftLog{
		Entries: make([]Entry, 1),
		Index0:  0,
	}
}

func (l *raftLog) startLog() *Entry {
	return l.at(0)
}

func (l *raftLog) lastLog() *Entry {
	if l.len() == 0 {
		return nil
	}
	return l.at(l.len() - 1)
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
