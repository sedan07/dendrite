package main

import (
	"./src/dendrite"
	"bufio"
	"flag"
	"github.com/fizx/logs"
	"io"
	"log"
	"os"
)

var configFile = flag.String("f", "/etc/dendrite/config.yaml", "location of the config file")
var debug = flag.Bool("d", false, "log at DEBUG")
var logFile = flag.String("l", "/var/log/dendrite.log", "location of the log file")

func main() {
	flag.Parse()

	// set the logger path
	handle, err := os.OpenFile(*logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		logs.Warn("Unable to open log file %s, using stderr: %s", *logFile, err)
	} else {
		logs.Logger = log.New(handle, "", log.LstdFlags|log.Lshortfile)
	}

	// Check whether we're in debug mode
	if *debug {
		logs.SetLevel(logs.DEBUG)
		logs.Debug("logging at DEBUG")
	} else {
		logs.SetLevel(logs.INFO)
	}

	// Read the config file, and link up all of the objects
	config := dendrite.NewConfig(*configFile)
	rw := config.CreateReadWriter()
	groups := config.CreateAllGroups(rw)

	// If any of our destinations talk back, log it.
	go func() {
		reader := bufio.NewReader(rw)
		for {
			str, err := reader.ReadString('\n')
			if err == io.EOF {
				logs.Debug("eof")
			} else if err != nil {
				logs.Error("error reading: %s", err)
			} else {
				logs.Info("received: %s", str)
			}
		}
	}()

	// Do the event loop
	groups.Loop()
}
