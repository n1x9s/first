cmake_minimum_required(VERSION 3.10)
project(ClickHouseExample)

set(CMAKE_CXX_STANDARD 20)

find_package(Boost REQUIRED)
find_package(Threads REQUIRED)
find_package(absl REQUIRED)

# Укажите пути к заголовочным файлам clickhouse-cpp и abseil
include_directories(/usr/local/include)
include_directories(/usr/local/include/absl)

# Укажите пути к библиотекам clickhouse-cpp, abseil, cityhash, lz4 и zstd
link_directories(/usr/local/lib)

add_executable(ClickHouseExample main.cpp)

# Укажите библиотеки для линковки
target_link_libraries(ClickHouseExample clickhouse-cpp-lib absl_synchronization absl_strings absl_base absl_int128 cityhash lz4 zstd ${Boost_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})
