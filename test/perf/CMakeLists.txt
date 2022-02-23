file(GLOB SRC_LIST "*.cpp")
file(GLOB HEADERS "*.h")

find_package(Boost CONFIG QUIET REQUIRED program_options)
find_package(Boost CONFIG QUIET REQUIRED serialization)

add_executable(keyRow_benchmark keyRow_benchmark.cpp)
target_compile_options(keyRow_benchmark PRIVATE -Wno-error)
target_include_directories(keyRow_benchmark PRIVATE . ../../bcos-storage)
target_link_libraries(keyRow_benchmark PUBLIC ${STORAGE_TARGET} bcos-framework::utilities bcos-framework::storage
        Boost::program_options)

add_executable(keyPage_benchmark keyPage_benchmark.cpp)
target_compile_options(keyPage_benchmark PRIVATE -Wno-error)
target_include_directories(keyPage_benchmark PRIVATE . ../../bcos-storage)
target_link_libraries(keyPage_benchmark PUBLIC ${STORAGE_TARGET} bcos-framework::utilities bcos-framework::storage
        Boost::program_options Boost::serialization)