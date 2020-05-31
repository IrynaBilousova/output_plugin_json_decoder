cmake_minimum_required (VERSION 3.15.0)

project (psql_mongo_replication LANGUAGES CXX)

if(DEFINED SMAKE_PREFIX_PATH)
    SET(CMAKE_PREFIX_PATH ${SMAKE_PREFIX_PATH};${CMAKE_PREFIX_PATH})
endif()

include(FetchContent)
include(CMakePrintHelpers)
include (externals/cmake_tools.txt)#must be above 
include (externals/jansson.txt)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# psql_mongo_replication
add_subdirectory(decoder_json)

add_custom_target(plugin_install COMMAND make install
                   WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/decoder_json
)