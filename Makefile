EXTENSION = tpch
DATA = tpch--1.0.sql
REGRESS = gen_schema gen_query explain

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

install: build-dbgen install-tpch-dbgen

build-dbgen:
	$(MAKE) -C TPC-H-V3.0.1/dbgen

install-tpch-dbgen:
	mkdir -p $(DESTDIR)$(datadir)/extension/tpch_dbgen
	cp -r TPC-H-V3.0.1/* $(DESTDIR)$(datadir)/extension/tpch_dbgen/

clean: clean-dbgen

clean-dbgen:
	$(MAKE) -C TPC-H-V3.0.1/dbgen clean

uninstall: uninstall-tpch-dbgen

uninstall-tpch-dbgen:
	psql -c "DROP EXTENSION IF EXISTS tpch CASCADE; DROP SCHEMA IF EXISTS tpch CASCADE;"
	rm -rf $(DESTDIR)$(datadir)/extension/tpch_dbgen
	rm -f $(DESTDIR)$(datadir)/extension/tpch.control
	rm -f $(DESTDIR)$(datadir)/extension/tpch--1.0.sql
