#ifndef __DECODER_JSON_FILTER_H__
#define __DECODER_JSON_FILTER_H__
#include <stdbool.h>

enum {DB_INDEXES_SIZE = 10};

bool decoder_json_filter_init();

bool decoder_json_filter_get_db_for_publication_table(      
      const char* table_name
    , int* db_instances
    , unsigned int db_instances_size
    , int* db_find_count);

#endif