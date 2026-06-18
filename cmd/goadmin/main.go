package main

import (
	"context"
	"os"

	"rocketmq-go-dashboard/internal/config"
	"rocketmq-go-dashboard/internal/goadmin"
)

func main() {
	options := goadmin.OptionsFromConfig(config.Load())
	os.Exit(goadmin.Run(context.Background(), options, os.Args[1:]))
}
