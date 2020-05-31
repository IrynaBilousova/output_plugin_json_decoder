#include "postgres.h"
#include "access/genam.h"
#include "access/sysattr.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "nodes/parsenodes.h"
#include "replication/output_plugin.h"
#include "replication/logical.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "utils/array.h"
#include "executor/spi.h"
#include <assert.h>
#include <string.h>

enum
{
    DB_ID_COLUMN = 1
};

static bool decoder_json_filter_find_pg_publication(
      const char* real_name
    , int* db_instances
    , unsigned int db_instances_size
    , int* db_find_count)
{
    if(!db_instances || !real_name || !db_find_count) return false;

    char query[300];

    char schema_name[20];
    char* table_name = strstr(real_name, ".");
    strncpy(schema_name, real_name, strlen(real_name) - strlen(table_name));
    table_name++;

    sprintf(query, "select si.subscriber_id from psql_to_mongo_replication.subscription_info as si join pg_publication_tables as ppt on ppt.pubname = si.pubname where ppt.tablename = '%s' and ppt.schemaname = '%s';", table_name, schema_name);
    elog(INFO, "decoder_json_filter_find_pg_publication: query [%s]", query);

    int ret = SPI_exec(query, 0);

    if(ret < 0)
    {
        elog(ERROR, "decoder_json_filter_find_pg_publication: SPI_exec...error %s", SPI_result_code_string(ret));
        return false;
    }

    if(db_instances_size < SPI_processed) return false;

    if (ret < 0 || SPI_tuptable == NULL) return false;

    TupleDesc tupdesc = SPI_tuptable->tupdesc;
    SPITupleTable *tuptable = SPI_tuptable;

    int proc = SPI_processed;

    for (int j = 0; j < proc; j++)
    {
        HeapTuple tuple = tuptable->vals[j];

        // for (i = 1, buf[0] = 0; i <= tupdesc->natts; i++)
        //     SPI_getvalue(tuple, tupdesc, i),
        //             (i == tupdesc->natts) ? " " : " |");
        int is_null = 0;
        Datum val = SPI_getbinval(tuple, tupdesc, DB_ID_COLUMN, &is_null);
        elog(INFO, "SPI_getbinval is_null %d: %d\n", is_null, proc);

        if(is_null) continue;

        Oid id = SPI_gettypeid(tupdesc, DB_ID_COLUMN);

        Assert(id == OIDOID || id == INT8OID);

        db_instances[j] = DatumGetInt64(val);

        elog(INFO, "DB instanes for table %s: %d\n", real_name, db_instances[j]);
    }

    *db_find_count = proc;

    return true;
}

static bool decoder_json_filter_init_validate_schema_spi_unsave()
{
    int ret = SPI_execute("SELECT to_regclass('psql_to_mongo_replication');", true, 0);
    if (ret < 0)
    {
        elog(ERROR, "_PG_init: SPI_execute returned %s", SPI_result_code_string(ret));
	    return false;
    }

    ret = SPI_execute("SELECT to_regclass('psql_to_mongo_replication.subscribers');", true, 0);
    if (ret < 0)
    {
        elog(ERROR, "_PG_init: SPI_execute returned %s", SPI_result_code_string(ret));
	    return false;
    }

    ret = SPI_execute("SELECT to_regclass('psql_to_mongo_replication.subscription_info');", true, 0);
    if (ret < 0)
    {
        elog(ERROR, "_PG_init: SPI_execute returned %s", SPI_result_code_string(ret));
	    return false;
    }

    if (SPI_tuptable == NULL)
    {
        elog(ERROR, "_PG_init: SPI_execute returned %s", SPI_result_code_string(ret));
        return false;
    }

    elog(INFO, "SPI_processed: %d\n", SPI_processed);

    return SPI_processed != 0;
}

bool decoder_json_filter_init()
{
    SPI_connect();

    bool is_valid = decoder_json_filter_init_validate_schema_spi_unsave();
    elog(INFO, "decoder_json_filter_init %d\n", is_valid);

    SPI_finish();

    return is_valid;
}

bool decoder_json_filter_get_db_for_publication_table(
      const char* table_name
    , int* db_instances
    , unsigned int db_instances_size
    , int* db_find_count)
{
    SPI_connect();

    elog(INFO, "decoder_json_filter_get_db_for_publication_table %s\n", table_name);
    bool okey = decoder_json_filter_find_pg_publication(table_name, db_instances, db_instances_size, db_find_count);

    SPI_finish();

    return okey;
}
