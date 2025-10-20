# examples/simple_duckdb/Makefile

DB ?= .local/demo.duckdb
PROJECT ?= .

.PHONY: seed run dag test clean

seed:
	FF_ENGINE=duckdb FF_DUCKDB_PATH="$(DB)" fft seed "$(PROJECT)" --env dev

run:
	FF_ENGINE=duckdb FF_DUCKDB_PATH="$(DB)" fft run $(PROJECT) --env dev

dag:
	FF_ENGINE=duckdb FF_DUCKDB_PATH="$(DB)" fft dag $(PROJECT) --env dev --html

test:
	FF_ENGINE=duckdb FF_DUCKDB_PATH="$(DB)" fft test $(PROJECT) --env dev --select batch

clean:
	rm -rf .local $(PROJECT)/docs

utest:
	fft utest . --model mart_orders_enriched --case join_and_flag
	fft utest . --path tests/unit/users_enriched.yml

utest-all:
	fft utest .
