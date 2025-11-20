# Project Root Makefile
# Robust: same DB for seed/run/dag/test, CLI fallback, path-safe

SHELL := /bin/bash

FF_PROJECT ?= examples/simple_duckdb
FF_DB ?= $(FF_PROJECT)/.local/demo.duckdb
FF_ENV ?= dev

# Detect OS opener (macOS: open, Linux: xdg-open)
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
	OPENER := open
else
	OPENER := xdg-open
endif

# Absolute path to this Makefile so includes stay robust
MAKEFILE_DIR := $(dir $(abspath $(firstword $(MAKEFILE_LIST))))

include $(MAKEFILE_DIR)/Makefile.pipeline
include $(MAKEFILE_DIR)/Makefile.dev
include $(MAKEFILE_DIR)/Makefile.built
