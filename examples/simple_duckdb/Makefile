# examples/simple_duckdb/Makefile

DB ?= .local/demo.duckdb
PROJECT ?= .

.PHONY: seed run dag test clean

seed:
	FF_ENGINE=duckdb FF_DUCKDB_PATH="$(DB)" flowforge seed "$(PROJECT)" --env dev

run:
	FF_ENGINE=duckdb FF_DUCKDB_PATH="$(DB)" flowforge run $(PROJECT) --env dev

dag:
	FF_ENGINE=duckdb FF_DUCKDB_PATH="$(DB)" flowforge dag $(PROJECT) --env dev --html

test:
	FF_ENGINE=duckdb FF_DUCKDB_PATH="$(DB)" flowforge test $(PROJECT) --env dev --select batch

clean:
	rm -rf .local $(PROJECT)/docs

utest:
	flowforge utest . --model mart_orders_enriched --case join_and_flag
	flowforge utest . --path tests/unit/users_enriched.yml

utest-all:
	flowforge utest .
