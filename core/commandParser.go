package core

import (
	"inmemorydb/utils"
	"strconv"
	"strings"

	"golang.org/x/exp/slices"
)

var supportedConditionsforCmd = map[string][]string{
	"SET":   []string{"NX", "XX"},
	"QPUSH": []string{},
	"QPOP":  []string{},
}

const (
	SET   = "SET"
	GET   = "GET"
	QPUSH = "QPUSH"
	QPOP  = "QPOP"
	EX    = "EX"
	NX    = "NX"
	XX    = "XX"
)

type Operation struct {
	Cmd         *string
	Key         *string
	Value       *string
	Expiry      *int
	Condition   *string
	QueryString string
	QueueValues []string
}

type CommandParser struct {
	CommandString  string
	Query          Operation
	isCommandValid bool
	Error          error
}

func NewCommandParser(cmdString string) *CommandParser {
	cmdParser := &CommandParser{
		CommandString:  cmdString,
		Query:          Operation{},
		isCommandValid: false,
	}

	return cmdParser
}

func (p *CommandParser) Parse() {
	if p.CommandString == "" {
		return
	}

	cmdArr := strings.Split(p.CommandString, " ")
	cmdArrLen := len(cmdArr)
	if cmdArrLen == 0 {
		return
	}

	switch cmdArr[0] {
	case SET:
		// 6 is max and 3 is min length of SET command
		if cmdArrLen > 6 || cmdArrLen < 3 {
			return
		}

		// Parse cmd, key value pair only
		p.Query.Cmd = utils.StringP("Set")
		p.Query.Key = utils.StringP(cmdArr[1])
		p.Query.Value = utils.StringP(cmdArr[2])
		p.Query.QueryString = p.CommandString

		// Parse expiry and condition
		if cmdArrLen > 3 {
			for i := 3; i < cmdArrLen; i++ {

				// Parse expiry
				if i == 4 {
					// Expiry cannot be present after condition
					if p.Query.Condition != nil {
						return
					}

					// Check for correct keyword EX for parsing expiry
					if cmdArr[i-1] == EX {
						expiry, err := strconv.ParseInt(cmdArr[i], 0, 64)
						if err != nil {
							p.Error = err
							return
						}

						p.Query.Expiry = utils.IntP(int(expiry))
					} else {
						return
					}
				} else if i == (cmdArrLen-1) && (cmdArrLen == 4 || cmdArrLen == 6) {
					// Check for invalid condition keywords
					if !slices.Contains(supportedConditionsforCmd[SET], cmdArr[i]) {
						return
					}
					p.Query.Condition = utils.StringP(cmdArr[i])
				} else if i == 3 && cmdArr[i] == EX {
					continue
				} else {
					return
				}
			}
		}
		p.isCommandValid = true
	case GET:
		if cmdArrLen > 2 || cmdArrLen < 2 {
			return
		}
		p.Query.Cmd = utils.StringP("Get")
		p.Query.Key = utils.StringP(cmdArr[1])
		p.Query.QueryString = p.CommandString
		p.isCommandValid = true
	case QPUSH:
		if cmdArrLen < 3 {
			return
		}

		p.Query.Cmd = utils.StringP("QPUSH")
		p.Query.Key = utils.StringP(cmdArr[1])
		p.Query.QueueValues = cmdArr[2:]
		p.Query.QueryString = p.CommandString
		p.isCommandValid = true

	case QPOP:
		if cmdArrLen != 2 {
			return
		}

		p.Query.Cmd = utils.StringP("QPOP")
		p.Query.Key = utils.StringP(cmdArr[1])
		p.Query.QueryString = p.CommandString
		p.isCommandValid = true

	default:
		return
	}

}

func (p *CommandParser) IsValid() bool {
	return p.isCommandValid
}

func (p *CommandParser) Err() error {
	return p.Error
}
