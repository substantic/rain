cmake_minimum_required(VERSION 3.1)
project(rainsw)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_EXTENSIONS OFF)

include(ExternalProject)
include(FindPkgConfig)


# spdlog
set(SPDLOG_ROOT ${CMAKE_CURRENT_BINARY_DIR}/spdlog)
ExternalProject_Add(spdlog
        PREFIX ${SPDLOG_ROOT}
        GIT_REPOSITORY https://github.com/gabime/spdlog
        GIT_TAG master
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
add_library(rainsw
  src/connection.cpp
  src/connection.h
  src/subworker.cpp
  src/subworker.h
  src/ids.cpp
  src/ids.h
  src/utils.cpp
  src/utils.h
  src/log.cpp
  src/log.h
)

target_include_directories(rainsw PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_include_directories(rainsw PRIVATE
        ${SPDLOG_ROOT}/src/spdlog/include
	#        ${LIBCBOR_ROOT}/src/libcbor/src
	#        ${LIBCBOR_ROOT}/src/libcbor-build
)
#add_dependencies(rainsw spdlog libcbor)


add_executable(tester
    tester/main.cpp)

target_include_directories(tester PUBLIC ${CBOR_INCLUDE_DIRS} ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries (tester rainsw ${CBOR_LIBRARIES} pthread)
