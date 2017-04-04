//
// Copyright 2017, TCN Inc.
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of TCN Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package sqlspanner

import (
	"database/sql/driver"
	"fmt"
	"strconv"

	"github.com/xwb1989/sqlparser"
)


type Args struct {
	Cur    int
	Counter int
}

type ArgPlaceholder struct {
	queuePos int
}

func (a *Args) ParseValExpr(expr sqlparser.ValExpr) (interface{}, error) {
	switch value := expr.(type) {
	case sqlparser.StrVal: // a quoted string
		fmt.Printf("StrVal %+v\n", value)
		return string(value[:]), nil
	case sqlparser.NumVal:
		fmt.Printf("NumVal %+v\n", value)
		rv, err := strconv.ParseInt(string(value[:]), 10, 64)
		if err != nil {
			rv, err := strconv.ParseFloat(string(value[:]), 64)
			if err != nil {
				return nil, fmt.Errorf("could not parse number value as int or float")
			}
			return rv, nil
		} else {
			return rv, nil
		}
	case sqlparser.ValArg: // ? arg to be supplied by the user
		val := ArgPlaceholder{queuePos: a.Counter}
		a.Counter += 1
		return val, nil
	case *sqlparser.NullVal:
		return nil, nil
	case *sqlparser.ColName:
		fmt.Printf("ColName %+v\n", value)
	case sqlparser.ValTuple:
		fmt.Printf("ValTuple %+v\n", value)
	case *sqlparser.Subquery:
		fmt.Printf("Subquery %+v\n", value)
	case sqlparser.ListArg:
		fmt.Printf("ListArg %+v\n", value)
	case *sqlparser.BinaryExpr:
		fmt.Printf("BinaryExpr %+v\n", value)
	case *sqlparser.UnaryExpr:
		fmt.Printf("UnaryExpr %+v\n", value)
	case *sqlparser.FuncExpr:
		fmt.Printf("FuncExpr %+v\n", value)
	case *sqlparser.CaseExpr:
		fmt.Printf("CaseExpr %+v\n", value)
	}
	return nil, fmt.Errorf("unsupported value expression")
}

type partialArgSlice struct {
	args []interface{}
	unfilled map[int]ArgPlaceholder
	expectedArgs int
}

type partialArgMap struct {
	args map[string]interface{}
	unfilled map[string]ArgPlaceholder
	expectedArgs int
}

func newPartialArgSlice() *partialArgSlice {
	return &partialArgSlice{
		args: make([]interface{}, 0),
		unfilled: make(map[int]ArgPlaceholder),
	}
}

func newPartialArgMap() *partialArgMap {
	return &partialArgMap{
		args: make(map[string]interface{}),
		unfilled: make(map[string]ArgPlaceholder),
	}
}

func (p *partialArgSlice) AddArgs(args ...interface{}) {
	for _, arg := range args {
		p.args = append(p.args, arg)
		if ap, ok := arg.(ArgPlaceholder); ok {
			p.unfilled[len(p.args) - 1] = ap
			p.expectedArgs += 1
		}
	}
}

func (p *partialArgSlice) GetFilledArgs(a []driver.Value) ([]interface{}, error) {
	if len(a) < p.expectedArgs {
		return nil, fmt.Errorf("expected at least %i args", p.expectedArgs)
	}
	argsCopy := p.args[:]
	for index, ap := range p.unfilled {
		argsCopy[index] = a[ap.queuePos]
	}
	return argsCopy, nil
}

func (p *partialArgMap) AddArg(key string, val interface{}) {
	p.args[key] = val
	if ap, ok := val.(ArgPlaceholder); ok {
		p.expectedArgs += 1
		p.unfilled[key] = ap
	}
}

func (p *partialArgMap) GetFilledArgs(a []driver.Value) (map[string]interface{}, error) {
	if len(a) < p.expectedArgs {
		return nil, fmt.Errorf("expected at least %i args", p.expectedArgs)
	}
	// this is modifiying the map directly, but should be okay because of the error above
	for key, ap := range p.unfilled {
		p.args[key] = a[ap.queuePos]
	}
	return p.args, nil
}

