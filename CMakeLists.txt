cmake_minimum_required(VERSION 3.13)

project(redis-starter-cpp)

# Generate compile_commands.json for language servers
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

file(GLOB_RECURSE SOURCE_FILES src/*.cpp)

set(CMAKE_CXX_STANDARD 23) # Enable the C++23 standard
set(THREADS_PREFER_PTHREAD_FLAG ON)

find_package(Threads REQUIRED)

add_executable(server ${SOURCE_FILES})

target_include_directories(server PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)

target_link_libraries(server PRIVATE Threads::Threads)
if(WIN32)
    target_link_libraries(server PRIVATE ws2_32)
endif()