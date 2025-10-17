.PHONY: up down seed run dag test

PG_DSN?=postgresql+psycopg://postgres:postgres@localhost:5432/ffdb

up:
	docker run --rm -d --name ff-pg -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=ffdb -p 5432:5432 postgres:16

down:
	-docker stop ff-pg

# seed:
# 	psql postgresql://postgres:postgres@localhost:5432/ffdb -c "create table if not exists seed_users(id int primary key, email text);"
# 	psql postgresql://postgres:postgres@localhost:5432/ffdb -c "insert into seed_users values (1,'a@example.com') on conflict do nothing;"
# 	psql postgresql://postgres:postgres@localhost:5432/ffdb -c "insert into seed_users values (2,'b@gmail.com') on conflict do nothing;"
# 	psql postgresql://postgres:postgres@localhost:5432/ffdb -c "insert into seed_users values (3,'c@gmail.com') on conflict do nothing;"

seed:
	FF_ENGINE=postgres FF_PG_DSN="$(PG_DSN)" flowforge seed "$(PROJECT)" --env stg

run:
	FF_ENGINE=postgres FF_PG_DSN=$(PG_DSN) flowforge run examples/postgres --env stg

dag:
	FF_ENGINE=postgres FF_PG_DSN=$(PG_DSN) flowforge dag examples/postgres --env stg --html

test:
	FF_ENGINE=postgres FF_PG_DSN=$(PG_DSN) flowforge test examples/postgres --env stg --select batch
