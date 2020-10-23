cmake_minimum_required(VERSION 3.2)

project(rocc)

option( LINK_STATIC_LIB "Link static version of libssmalloc and libboost" true)

#set(CMAKE_CXX_COMPILER /spack/apps/gcc/8.3.0/bin/g++)
#set(CMAKE_C_COMPILER /spack/apps/gcc/8.3.0/bin/gcc)
#set(CMAKE_CXX_COMPILER /usr/bin/g++)
#set(CMAKE_C_COMPILER /usr/bin/gcc)
#set(CMAKE_CXX_COMPILER g++-4.8)
set(ROCC_ROOT "." )
#set(CMAKE_BUILD_TYPE Debug)

ADD_DEFINITIONS( -std=c++0x)

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

set(CMAKE_THREAD_LIBS_INIT "-lpthread")
set(CMAKE_HAVE_THREADS_LIBRARY 1)
set(CMAKE_USE_WIN32_THREADS_INIT 0)
set(CMAKE_USE_PTHREADS_INIT 1)
set(THREADS_PREFER_PTHREAD_FLAG ON)

## TODO, we shall replace it with a pre-complied lib, but since now the lib is not stable, so we just add sources here
#file(GLOB RDMA_SOURCES "third_party/libRDMA/src/*.cc" "third_party/micautil/*.cc")
file(GLOB RDMA_SOURCES  "third_party/micautil/*.cc")

## add main files
file(GLOB SOURCES
          "src/app/micro/*.cc" "src/app/tpcc/*.cc"  "src/app/smallbank/*.cc"                     # apps
          #"src/app/micro_benches/*.cc" "src/app/tpcc/*.cc"  "src/app/smallbank/*.cc"
          "src/app/tpce/*.cc"                                                                    # apps continued
          "src/app/graph/*.cc"                                                                   # apps continued 
          "src/framework/*.cc"  "src/framework/utils/*.cc"                                       # framework
          "src/memstore/*.cc"                                                                    # memstore
          "src/core/*.cc"                                                                        # nocc core
          "src/db/*.cc" "src/db/txs/*.cc"                                                        # framework + CC
          "src/util/*.cc" "src/util/micautil/*.cc" "src/port/*.cc" "src/oltp/utils/*.cc"         # utils
          "src/rtx/*.cc"       # new rtx's code
          )

include(cmake/tpce.cmake)
add_executable(noccocc ${SOURCES} ${TPCE_SOURCES} ${RDMA_SOURCES})
#target_include_directories(noccocc BEFORE PRIVATE /usr/include/c++/4.8.5/ PRIVATE /usr/lib/gcc/x86_64-redhat-linux/4.8.5/include/ /spack/apps/linux-centos7-x86_64/gcc-8.3.0/rdma-core-20-4oz3lq23c2m4adoovlu4akm6nugs6kfs/include/)
#target_include_directories(noccocc BEFORE PRIVATE /spack/apps/gcc/9.2.0/include/c++/9.2.0/ PRIVATE /spack/apps/gcc/9.2.0/lib/gcc/x86_64-pc-linux-gnu/9.2.0/include/ PRIVATE /spack/apps/linux-centos7-x86_64/gcc-9.2.0/rdma-core-20-u3xrik4xivt5t5q74vv3f5lbvzuc526d)
#target_include_directories(noccocc BEFORE PRIVATE /spack/apps/linux-centos7-x86_64/gcc-9.2.0/rdma-core-20-u3xrik4xivt5t5q74vv3f5lbvzuc526d/include/)
target_include_directories(noccocc BEFORE PRIVATE /spack/apps/linux-centos7-x86_64/gcc-8.3.0/rdma-core-20-4oz3lq23c2m4adoovlu4akm6nugs6kfs/include/)

target_compile_options(noccocc PRIVATE "-DOCC_TX")

## Install static dependencies
include(cmake/Dependencies.cmake)

#
# Configure binary linking
#
set(apps noccocc)
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
install(TARGETS noccocc  DESTINATION scripts) 


## for tests
if(GTEST)
    include(cmake/gtest.cmake)
    enable_testing()
    include(cmake/unit_tests.cmake)
endif()
