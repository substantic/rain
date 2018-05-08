cmake_minimum_required(VERSION 3.1)
project(rainsw)

# Create a library called "Hello" which includes the source file "hello.cxx".
# The extension is already found. Any number of sources could be listed here.
add_library (rainsw
  src/connection.cpp
  src/connection.h
  src/subworker.cpp
  src/subworker.h
  src/log.cpp
  src/log.h
)

target_include_directories (rainsw PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
