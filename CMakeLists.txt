cmake_minimum_required(VERSION 3.2)

project(rocc)

option( LINK_STATIC_LIB "Link static version of libssmalloc and libboost" true)

set(ROCC_ROOT "." )
#set(CMAKE_BUILD_TYPE Debug)

ADD_DEFINITIONS( -std=c++0x )

#set(CMAKE_CXX_COMPILER g++-4.8)
include_directories(./src)
include_directories(.)

## third party libraries
include_directories(third_party/sparsehash-c11)
include_directories(third_party)


#with benchmark profiling
#set(MACRO_FLAGS "-DNDEBUG -DBASE_LINE -DBENCH_PROFILE")

include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/rocc.cmake)
load_global_config()
load_tx_config()


## seperate install
set(CMAKE_SKIP_INSTALL_ALL_DEPENDENCY true)

## without benchmark profiling 
#set(MACRO_FLAGS "-DNDEBUG -DBASE_LINE ")  ## ndebug will optimze out assert
set (MACRO_FLAGS "-DBASE_LINE")

## currently drtm in this codebase is not supported, i will fix this later 
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DLEVELDB_PLATFORM_POSIX -pthread -DOS_LINUX -mrtm -pthread -O2 -g ${MACRO_FLAGS}")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DLEVELDB_PLATFORM_POSIX -pthread -DOS_LINUX -mrtm -pthread  -O0 -g2 ${MACRO_FLAGS}")

## TODO, we shall replace it with a pre-complied lib, but since now the lib is not stable, so we just add sources here
file(GLOB RDMA_SOURCES "third_party/libRDMA/src/*.cc" "third_party/micautil/*.cc")

## add main files
file(GLOB SOURCES
          "src/app/micro_benches/*.cc" "src/app/tpcc/*.cc"  "src/app/smallbank/*.cc"             # apps
          # "src/app/tpce/*.cc"                                                                    # apps continued
          "src/app/graph/*.cc"                                                                   # apps continued 
          "src/framework/*.cc"  "src/framework/utils/*.cc"                                       # framework
          "src/memstore/*.cc"                                                                    # memstore
          "src/core/*.cc"                                                                        # nocc core
          "src/db/*.cc" "src/db/txs/*.cc"                                                        # framework + CC
          "src/util/*.cc" "src/util/micautil/*.cc" "src/port/*.cc" "src/oltp/utils/*.cc"         # utils
          "src/rtx/*.cc"       # new rtx's code
          )

include(cmake/tpce.cmake)

# add_executable(noccocc ${SOURCES} ${TPCE_SOURCES} ${RDMA_SOURCES})
add_executable(noccocc ${SOURCES} ${RDMA_SOURCES})
target_compile_options(noccocc PRIVATE "-DOCC_TX")

add_executable(noccocc-tcp ${SOURCES} ${RDMA_SOURCES})
target_compile_options(noccocc-tcp PRIVATE "-DOCC_TX")

add_executable(noccocc-rpc ${SOURCES} ${RDMA_SOURCES})
target_compile_options(noccocc-rpc PRIVATE "-DOCC_TX")

add_executable(noccocc-onesided ${SOURCES} ${RDMA_SOURCES})
target_compile_options(noccocc-onesided PRIVATE "-DOCC_TX")

add_executable(noccocc-hybrid ${SOURCES} ${RDMA_SOURCES})
target_compile_options(noccocc-hybrid PRIVATE "-DOCC_TX")

# add_executable(noccnowait ${SOURCES} ${TPCE_SOURCES} ${RDMA_SOURCES})

add_executable(noccnowait ${SOURCES} ${RDMA_SOURCES})
target_compile_options(noccnowait PRIVATE "-DNOWAIT_TX")

add_executable(noccnowait-tcp ${SOURCES} ${RDMA_SOURCES})
target_compile_options(noccnowait-tcp PRIVATE "-DNOWAIT_TX")

add_executable(noccnowait-rpc ${SOURCES} ${RDMA_SOURCES})
target_compile_options(noccnowait-rpc PRIVATE "-DNOWAIT_TX")

add_executable(noccnowait-onesided ${SOURCES} ${RDMA_SOURCES})
target_compile_options(noccnowait-onesided PRIVATE "-DNOWAIT_TX")

add_executable(noccnowait-hybrid ${SOURCES} ${RDMA_SOURCES})
target_compile_options(noccnowait-hybrid PRIVATE "-DNOWAIT_TX")

# add_executable(noccwaitdie ${SOURCES} ${TPCE_SOURCES} ${RDMA_SOURCES})
add_executable(noccwaitdie ${SOURCES} ${RDMA_SOURCES})
target_compile_options(noccwaitdie PRIVATE "-DWAITDIE_TX")

add_executable(noccwaitdie-tcp ${SOURCES} ${RDMA_SOURCES})
target_compile_options(noccwaitdie-tcp PRIVATE "-DWAITDIE_TX")

add_executable(noccwaitdie-rpc ${SOURCES} ${RDMA_SOURCES})
target_compile_options(noccwaitdie-rpc PRIVATE "-DWAITDIE_TX")

add_executable(noccwaitdie-onesided ${SOURCES} ${RDMA_SOURCES})
target_compile_options(noccwaitdie-onesided PRIVATE "-DWAITDIE_TX")

add_executable(noccwaitdie-hybrid ${SOURCES} ${RDMA_SOURCES})
target_compile_options(noccwaitdie-hybrid PRIVATE "-DWAITDIE_TX")

add_executable(noccsundial ${SOURCES} ${RDMA_SOURCES})
target_compile_options(noccsundial PRIVATE "-DSUNDIAL_TX")

add_executable(noccsundial-tcp ${SOURCES} ${RDMA_SOURCES})
target_compile_options(noccsundial-tcp PRIVATE "-DSUNDIAL_TX")

add_executable(noccsundial-rpc ${SOURCES} ${RDMA_SOURCES})
target_compile_options(noccsundial-rpc PRIVATE "-DSUNDIAL_TX")

add_executable(noccsundial-onesided ${SOURCES} ${RDMA_SOURCES})
target_compile_options(noccsundial-onesided PRIVATE "-DSUNDIAL_TX")

add_executable(noccsundial-hybrid ${SOURCES} ${RDMA_SOURCES})
target_compile_options(noccsundial-hybrid PRIVATE "-DSUNDIAL_TX")


add_executable(noccmvcc ${SOURCES} ${RDMA_SOURCES})
target_compile_options(noccmvcc PRIVATE "-DMVCC_TX")

add_executable(noccmvcc-tcp ${SOURCES} ${RDMA_SOURCES})
target_compile_options(noccmvcc-tcp PRIVATE "-DMVCC_TX")

add_executable(noccmvcc-rpc ${SOURCES} ${RDMA_SOURCES})
target_compile_options(noccmvcc-rpc PRIVATE "-DMVCC_TX")

add_executable(noccmvcc-onesided ${SOURCES} ${RDMA_SOURCES})
target_compile_options(noccmvcc-onesided PRIVATE "-DMVCC_TX")

add_executable(noccmvcc-hybrid ${SOURCES} ${RDMA_SOURCES})
target_compile_options(noccmvcc-hybrid PRIVATE "-DMVCC_TX")


# add_executable(nocccalvin ${SOURCES} ${TPCE_SOURCES} ${RDMA_SOURCES})
add_executable(nocccalvin ${SOURCES} ${RDMA_SOURCES})
target_compile_options(nocccalvin PRIVATE "-DCALVIN_TX")

add_executable(nocccalvin-rpc ${SOURCES} ${RDMA_SOURCES})
target_compile_options(nocccalvin-rpc PRIVATE "-DCALVIN_TX")

add_executable(nocccalvin-onesided ${SOURCES} ${RDMA_SOURCES})
target_compile_options(nocccalvin-onesided PRIVATE "-DCALVIN_TX")

add_executable(nocccalvin-hybrid ${SOURCES} ${RDMA_SOURCES})
target_compile_options(nocccalvin-hybrid PRIVATE "-DCALVIN_TX")

add_executable(noccsi ${SOURCES} ${TPCE_SOURCES} ${RDMA_SOURCES})
target_compile_options(noccsi PRIVATE "-DSI_TX")

add_executable(noccfarm ${SOURCES} ${TPCE_SOURCES} ${RDMA_SOURCES})
target_compile_options(noccfarm PRIVATE "-DFARM")

add_executable(noccrad ${SOURCES} ${TPCE_SOURCES} ${RDMA_SOURCES})
target_compile_options(noccrad PRIVATE "-DRAD_TX")

## Micro with one-sided RDMA
add_executable(nn ${SOURCES} ${TPCE_SOURCES} ${RDMA_SOURCES})
target_compile_options(nn PRIVATE "-DRPC=0" "-DOCC_TX")

# Micro with RDMA RPC
add_executable(nr ${SOURCES} ${TPCE_SOURCES} ${RDMA_SOURCES})
target_compile_options(nr PRIVATE "-DRPC=1" "-DOCC_TX")

## Install static dependencies
include(cmake/Dependencies.cmake)

#
# Configure binary linking
#
set( apps noccrad noccocc noccocc-rpc noccocc-onesided noccocc-hybrid noccsi noccfarm noccnowait noccnowait-rpc noccnowait-onesided noccnowait-hybrid noccsundial-rpc noccsundial-onesided noccsundial-hybrid noccsundial noccmvcc noccmvcc-rpc noccmvcc-onesided noccmvcc-hybrid noccwaitdie noccwaitdie-rpc noccwaitdie-onesided noccwaitdie-hybrid nocccalvin nocccalvin-rpc nocccalvin-onesided nocccalvin-hybrid nn nr )
foreach( prog ${apps} )
  if( LINK_STATIC_LIB )
    target_link_libraries( ${prog}
      ${LIBZMQ} rt ${LIBIBVERBS}
      ssmalloc
      boost_coroutine boost_chrono boost_thread boost_context boost_system cpuinfo)
  else()
    target_link_libraries( ${prog}
      ${LIBZMQ} rt ${LIBIBVERBS}
      ssmalloc
      boost_coroutine boost_system )
  endif()
  add_dependencies( ${prog} ralloc libboost1.61 )
  add_custom_command(TARGET ${prog}
    POST_BUILD
    COMMAND mv ${prog} ${CMAKE_SOURCE_DIR}/scripts
    )
endforeach( prog )

# for the ease of running
set(CMAKE_INSTALL_PREFIX ./)
install(TARGETS noccocc  	DESTINATION scripts) 
install(TARGETS noccocc-rpc  	DESTINATION scripts) 
install(TARGETS noccocc-onesided  	DESTINATION scripts) 
install(TARGETS noccocc-hybrid  	DESTINATION scripts) 
install(TARGETS noccfarm 	DESTINATION scripts)
install(TARGETS noccsi   	DESTINATION scripts)
install(TARGETS noccnowait 	DESTINATION scripts)
install(TARGETS noccnowait-rpc 	DESTINATION scripts)
install(TARGETS noccnowait-onesided 	DESTINATION scripts)
install(TARGETS noccnowait-hybrid 	DESTINATION scripts)
install(TARGETS noccwaitdie   	DESTINATION scripts)
install(TARGETS noccwaitdie-rpc   	DESTINATION scripts)
install(TARGETS noccwaitdie-onesided   	DESTINATION scripts)
install(TARGETS noccwaitdie-hybrid   	DESTINATION scripts)
install(TARGETS noccsundial   	DESTINATION scripts)
install(TARGETS noccsundial-rpc   	DESTINATION scripts)
install(TARGETS noccsundial-onesided   	DESTINATION scripts)
install(TARGETS noccsundial-hybrid   	DESTINATION scripts)
install(TARGETS noccmvcc     DESTINATION scripts)
install(TARGETS noccmvcc-rpc     DESTINATION scripts)
install(TARGETS noccmvcc-hybrid     DESTINATION scripts)
install(TARGETS noccmvcc-onesided     DESTINATION scripts)
install(TARGETS nocccalvin  DESTINATION scripts)
install(TARGETS nocccalvin-rpc  DESTINATION scripts)
install(TARGETS nocccalvin-onesided  DESTINATION scripts)
install(TARGETS nocccalvin-hybrid  DESTINATION scripts)

## for tests
if(GTEST)
    include(cmake/gtest.cmake)
    enable_testing()
    include(cmake/unit_tests.cmake)
endif()
