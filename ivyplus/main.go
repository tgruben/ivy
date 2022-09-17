package main

import (
	"io/ioutil"
	"os"
	"strings"

	"github.com/chzyer/readline"
	"robpike.io/ivy/config"
	"robpike.io/ivy/exec"
	"robpike.io/ivy/parse"
	"robpike.io/ivy/run"
	"robpike.io/ivy/scan"
	"robpike.io/ivy/value"
)

func runString(context value.Context, str string) ([]value.Value, bool) {
	scanner := scan.New(context, "<args>", strings.NewReader(str))
	p := parse.NewParser("<args>", scanner, context)
	exprs, ok := p.Line()
	if !ok {
		return nil, ok
	}
	values := context.Eval(exprs)
	return values, ok
}

func getDefaultConfig() config.Config {
	maxbits := uint(1e9)   // "maximum size of an integer, in bits; 0 means no limit")
	maxdigits := uint(1e4) // "above this many `digits`, integers print as floating point; 0 disables")
	maxstack := uint(100000)
	origin := 1  // "set index origin to `n` (must be 0 or 1)")
	prompt := "" // flag.String("prompt", "", "command `prompt`")
	format := ""
	//      debugFlag := "" // flag.String("debug", "", "comma-separated `names` of debug settings to enable")
	conf := config.Config{}
	conf.SetFormat(format)
	conf.SetMaxBits(maxbits)
	conf.SetMaxDigits(maxdigits)
	conf.SetMaxStack(maxstack)
	conf.SetOrigin(origin)
	conf.SetPrompt(prompt)
	conf.SetOutput(ioutil.Discard)
	return conf
}

func main() {
	rl, err := readline.NewEx(&readline.Config{
		Prompt:                 "> ",
		HistoryFile:            "/tmp/readline-multiline",
		DisableAutoSaveHistory: true,
	})
	if err != nil {
		panic(err)
	}
	defer rl.Close()
	conf := getDefaultConfig()
	context := exec.NewContext(&conf)
	writer := os.Stdout
	var cmds []string
	for {
		line, err := rl.Readline()
		if err != nil {
			break
		}
		line = strings.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		cmds = append(cmds, line)
		if !strings.HasSuffix(line, ";") {
			rl.SetPrompt(">>> ")
			continue
		}
		cmd := strings.Join(cmds, " ")
		cmd = cmd[:len(cmd)-1]
		cmds = cmds[:0]
		rl.SetPrompt("> ")
		rl.SaveHistory(cmd)
		println(cmd)
		values, ok := runString(context, cmd)
		if ok {
			if run.PrintValues(&conf, writer, values) {
				context.AssignGlobal("_", values[len(values)-1])
			}
		}
	}
}
