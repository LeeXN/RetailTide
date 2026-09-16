package main

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
)

// HTTP and MCP share one browser lane. Ownership lasts through page/browser cleanup.
var readOperations = make(chan struct{}, 1)

func beginReadOperation(parent context.Context, operation string) (context.Context, func(*error), error) {
	ctx, cancel := context.WithTimeout(parent, 45*time.Second)
	select {
	case readOperations <- struct{}{}:
	case <-ctx.Done():
		cancel()
		return nil, nil, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		<-readOperations
		cancel()
		return nil, nil, err
	}
	started := time.Now()
	logrus.WithField("operation", operation).Info("event=browser_operation_started")
	return ctx, func(resultErr *error) {
		if recovered := recover(); recovered != nil {
			// Rod errors can contain signed URLs; report the type, never the payload.
			*resultErr = fmt.Errorf("browser operation failed (%T)", recovered)
		}
		if ctx.Err() != nil {
			*resultErr = ctx.Err()
		}
		cancel()
		logrus.WithFields(logrus.Fields{"operation": operation, "elapsed_ms": time.Since(started).Milliseconds(),
			"failed": *resultErr != nil}).Info("event=browser_operation_closed")
		<-readOperations
	}, nil
}
