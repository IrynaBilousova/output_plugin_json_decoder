cmake_minimum_required (VERSION 3.15.0)

project (decoder_json LANGUAGES C)

add_library(decoder_json SHARED)

target_sources(decoder_json
    PRIVATE
    src/decoder_json/decoder_json.c
)

include_directories(
    ${INCLUDEDIR-SERVER}
    ${PKGINCLUDEDIR}
    ${INCLUDEDIR}
    ${INCLUDEDIR}/internal
)

if(NOT DEFINED PKGLIBDIR)
    SET(PKGLIBDIR ${SMAKE_PREFIX_PATH})
endif()

if(NOT DEFINED PSQLLIBS)
    SET(PSQLLIBS ${SMAKE_PREFIX_PATH})
endif()

message(PKGLIBDIR "-${PKGLIBDIR}")
message(PSQLLIBS "${LIBS}")

#### Check all options and find answers why ####
#target_compile_options(decoder_json PRIVATE ${CFLAGS})

target_compile_options(decoder_json PRIVATE 
-Wall 
-Wmissing-prototypes 
-Wpointer-arith 
-Wdeclaration-after-statement 
-Werror=vla 
-Wendif-labels 
-Wmissing-format-attribute 
-Wformat-security 
-fno-strict-aliasing  
-fwrapv  
-fexcess-precision=standard  
-Wno-format-truncation  
-Wno-stringop-truncation  
-g -g -O2  
-fstack-protector-strong  
-Wformat  
-Werror=format-security  
-fno-omit-frame-pointer 
-fPIC
-pie)

message(LDFLAGS "${LDFLAGS}")

target_link_options(decoder_json PRIVATE 
-Wl,-Bsymbolic-functions 
-Wl,-z,relro 
-Wl,-z,now 
-L/usr/lib/llvm-10/lib 
-L/usr/lib/x86_64-linux-gnu/mit-krb5 
-Wl,--as-needed)

#target_link_options(decoder_json PRIVATE ${LDFLAGS})

target_link_libraries(decoder_json 
PRIVATE 
jansson
jansson_headers
pq)
