package raft

import (
	"cmp"
	"fmt"
	"log"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	color_red = uint8(iota + 91)
	color_green
	color_yellow
	color_blue
	color_magenta //洋红
	info          = "[INFO]"
	trac          = "[TRAC]"
	erro          = "[ERRO]"
	warn          = "[WARN]"
	succ          = "[SUCC]"
	node          = "Node Info > raft.Id : %d, raft.Term : %d, raft.Role : %s | "
)

// see complete color rules in document in https://en.wikipedia.org/wiki/ANSI_escape_code#cite_note-ecma48-13
func (rf *Raft) Trace(format string, a ...interface{}) {
	prefix := yellow(trac)
	var tempRole string
	if rf.Role == Follower {
		tempRole = "Follower"
	} else if rf.Role == Candidate {
		tempRole = "Candidate"
	} else if rf.Role == Leader {
		tempRole = "Leader"
	} else {
		tempRole = "Unkown"
	}
	tempStr := fmt.Sprintf(node, rf.me, rf.CurTerm, tempRole)

	if Debug {
		fmt.Println(formatLog(prefix), tempStr, fmt.Sprintf(format, a...))
	}

}
func (rf *Raft) Info(format string, a ...interface{}) {
	prefix := blue(info)

	var tempRole string
	if rf.Role == Follower {
		tempRole = "Follower"
	} else if rf.Role == Candidate {
		tempRole = "Candidate"
	} else if rf.Role == Leader {
		tempRole = "Leader"
	} else {
		tempRole = "Unkown"
	}
	tempStr := fmt.Sprintf(node, rf.me, rf.CurTerm, tempRole)
	if Debug {
		fmt.Println(formatLog(prefix), tempStr, fmt.Sprintf(format, a...))
	}

}
func (rf *Raft) Success(format string, a ...interface{}) {
	prefix := green(succ)
	var tempRole string
	if rf.Role == Follower {
		tempRole = "Follower"
	} else if rf.Role == Candidate {
		tempRole = "Candidate"
	} else if rf.Role == Leader {
		tempRole = "Leader"
	} else {
		tempRole = "Unkown"
	}
	tempStr := fmt.Sprintf(node, rf.me, rf.CurTerm, tempRole)
	if Debug {
		fmt.Println(formatLog(prefix), tempStr, fmt.Sprintf(format, a...))
	}
}
func (rf *Raft) Warning(format string, a ...interface{}) {
	prefix := magenta(warn)
	var tempRole string
	if rf.Role == Follower {
		tempRole = "Follower"
	} else if rf.Role == Candidate {
		tempRole = "Candidate"
	} else if rf.Role == Leader {
		tempRole = "Leader"
	} else {
		tempRole = "Unkown"
	}
	tempStr := fmt.Sprintf(node, rf.me, rf.CurTerm, tempRole)

	if Debug {
		fmt.Println(formatLog(prefix), tempStr, fmt.Sprintf(format, a...))
	}
}
func (rf *Raft) Error(format string, a ...interface{}) {
	prefix := red(erro)
	var tempRole string
	if rf.Role == Follower {
		tempRole = "Follower"
	} else if rf.Role == Candidate {
		tempRole = "Candidate"
	} else if rf.Role == Leader {
		tempRole = "Leader"
	} else {
		tempRole = "Unkown"
	}
	tempStr := fmt.Sprintf(node, rf.me, rf.CurTerm, tempRole)
	if Debug {
		fmt.Println(formatLog(prefix), tempStr, red(fmt.Sprintf(format, a...)))
	}
}

func red(s string) string {
	return fmt.Sprintf("\x1b[%dm%s\x1b[0m", color_red, s)
}
func green(s string) string {
	return fmt.Sprintf("\x1b[%dm%s\x1b[0m", color_green, s)
}
func yellow(s string) string {
	return fmt.Sprintf("\x1b[%dm%s\x1b[0m", color_yellow, s)
}
func blue(s string) string {
	return fmt.Sprintf("\x1b[%dm%s\x1b[0m", color_blue, s)
}
func magenta(s string) string {
	return fmt.Sprintf("\x1b[%dm%s\x1b[0m", color_magenta, s)
}
func formatLog(prefix string) string {
	return time.Now().Format("2006/01/02 15:04:05.000") + " " + prefix + " "
}

func Ternary(isTrue bool, first any, second any) any {
	if isTrue {
		fmt.Println("true, first")
		return first
	}
	fmt.Println("false, second")
	return second
}

func Min[T cmp.Ordered](a, b T) T {
	if a < b {
		return a
	}
	return b
}
