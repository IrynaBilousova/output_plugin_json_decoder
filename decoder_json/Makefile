PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
MODULE_big = decoder_json
OBJS = ./src/decoder_json/decoder_json.o ./src/decoder_json/decoder_json_filter.o 
JANSSON_VERSION = 2.7
SHLIB_LINK = -Wl,--version-script,../visibility.map ../build/_deps/jansson-build/lib/libjansson.a
PG_CPPFLAGS =  -I../build/_deps/jansson-build/include/
REGRESS = default
include $(PGXS)

test:
	- psql contrib_regression -c "SELECT pg_drop_replication_slot('custom_slot');"
	- rm `pg_config --pkglibdir`/decoder_json.so
	cp decoder_json.so `pg_config --pkglibdir`/
	make installcheck
