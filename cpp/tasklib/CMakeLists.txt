cmake_minimum_required(VERSION 3.1)
project(tasklib)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_EXTENSIONS OFF)
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -g")

include(ExternalProject)
include(FindPkgConfig)


# spdlog
set(SPDLOG_ROOT ${CMAKE_CURRENT_BINARY_DIR}/spdlog)
ExternalProject_Add(spdlog
        PREFIX ${SPDLOG_ROOT}
        GIT_REPOSITORY https://github.com/gabime/spdlog
        GIT_TAG 71e93a4f2d52fb9f9dafdeea7386bf0c780981e3
        CONFIGURE_COMMAND ""
        BUILD_COMMAND ""
        INSTALL_COMMAND ""
)

find_package(PkgConfig REQUIRED)
pkg_check_modules(CBOR REQUIRED libcbor)

# libcbor
# set(LIBCBOR_ROOT ${CMAKE_CURRENT_BINARY_DIR}/libcbor)
# ExternalProject_Add(libcbor
#       PREFIX ${LIBCBOR_ROOT}
#	GIT_REPOSITORY https://github.com/PJK/libcbor
#       GIT_TAG master
#       INSTALL_COMMAND ""
#)

# Create a library called "Hello" which includes the source file "hello.cxx".
# The extension is already found. Any number of sources could be listed here.
add_library(tasklib
  src/connection.cpp
  src/connection.h
  src/datainstance.cpp
  src/datainstance.h
  src/executor.cpp
  src/executor.h
  src/ids.cpp
  src/ids.h
  src/utils.cpp
  src/utils.h
  src/log.cpp
  src/log.h
  src/context.cpp
  src/context.h
)

target_include_directories(tasklib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_include_directories(tasklib PRIVATE
        ${SPDLOG_ROOT}/src/spdlog/include
	#        ${LIBCBOR_ROOT}/src/libcbor/src
	#        ${LIBCBOR_ROOT}/src/libcbor-build
)
add_dependencies(tasklib spdlog)


add_executable(tester
    tester/main.cpp)

target_include_directories(tester PUBLIC ${CBOR_INCLUDE_DIRS} ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries (tester tasklib ${CBOR_LIBRARIES} pthread)
