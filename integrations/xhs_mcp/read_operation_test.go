package main

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestReadOperationCanceledWaiterDoesNotOwnBrowser(t *testing.T) {
	_, release, err := beginReadOperation(context.Background(), "first")
	if err != nil {
		t.Fatal(err)
	}
	defer release(&err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, waitErr := beginReadOperation(ctx, "second")
	if !errors.Is(waitErr, context.Canceled) {
		t.Fatalf("got %v", waitErr)
	}
}

func TestReadOperationPanicReleasesAfterCleanup(t *testing.T) {
	cleaned := false
	run := func() (err error) {
		_, release, err := beginReadOperation(context.Background(), "detail")
		if err != nil {
			return err
		}
		defer release(&err)
		defer func() { cleaned = true }()
		panic("signed-url-must-not-be-logged")
	}
	err := run()
	if !cleaned || err == nil || strings.Contains(err.Error(), "signed-url") {
		t.Fatal("cleanup or redaction failed")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, release, err := beginReadOperation(ctx, "next")
	if err != nil {
		t.Fatal(err)
	}
	release(&err)
}
