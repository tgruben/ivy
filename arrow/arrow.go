package arrow

import (
	"errors"
	"fmt"
	"strings"

	"github.com/apache/arrow/go/v10/arrow"
	"robpike.io/ivy/config"
	"robpike.io/ivy/exec"
	"robpike.io/ivy/parse"
	"robpike.io/ivy/run"
	"robpike.io/ivy/scan"
	"robpike.io/ivy/value"
)

func RunArrow(table arrow.Table, computation string, conf config.Config, resolver value.Resolver) (context value.Context, err error) {
	/*
		conf.SetFormat(*format)
		conf.SetMaxBits(*maxbits)
		conf.SetMaxDigits(*maxdigits)
		conf.SetMaxStack(*maxstack)
		conf.SetOrigin(*origin)
		conf.SetPrompt(*prompt)
	*/

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("ivy error %v", r)
		}
	}()

	context = exec.NewContext(&conf)
	scanner := scan.New(context, "<args>", strings.NewReader(computation))
	parser := parse.NewParser("<args>", scanner, context)
	err = context.(*exec.Context).LoadGlobalsFromTable(table, &conf, resolver)
	if err != nil {
		return nil, err
	}

	worked := run.Run(parser, context, false)
	if worked {
		return context, nil
	}
	return nil, errors.New("didin't work")
}
