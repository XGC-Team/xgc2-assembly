add_library(xa_fixture MODULE fixture.c)
add_library(xa_bad_abi MODULE fixture.c)
add_library(xa_bad_size MODULE fixture.c)
add_library(xa_missing_callback MODULE fixture.c)
add_library(xa_exception_fixture MODULE exception_fixture.cpp)
add_library(xa_missing_entry MODULE missing_entry.c)
foreach(target xa_fixture xa_bad_abi xa_bad_size xa_missing_callback xa_exception_fixture)
  target_include_directories(${target} PRIVATE ../include)
  set_target_properties(${target} PROPERTIES C_VISIBILITY_PRESET hidden CXX_VISIBILITY_PRESET hidden)
endforeach()
target_link_libraries(xa_exception_fixture PRIVATE Threads::Threads)
target_compile_definitions(xa_bad_abi PRIVATE BAD_ABI=900)
target_compile_definitions(xa_bad_size PRIVATE BAD_SIZE=8)
target_compile_definitions(xa_missing_callback PRIVATE BAD_STEP=0)
add_executable(xa_tests tests.cpp)
target_link_libraries(xa_tests PRIVATE xgc2::assembly)
target_compile_options(xa_tests PRIVATE -Wall -Wextra -Wpedantic -Werror)
target_compile_definitions(xa_tests PRIVATE
  FIXTURE_PATH="$<TARGET_FILE:xa_fixture>"
  BAD_ABI_PATH="$<TARGET_FILE:xa_bad_abi>"
  BAD_SIZE_PATH="$<TARGET_FILE:xa_bad_size>"
  MISSING_CALLBACK_PATH="$<TARGET_FILE:xa_missing_callback>"
  EXCEPTION_PATH="$<TARGET_FILE:xa_exception_fixture>"
  MISSING_ENTRY_PATH="$<TARGET_FILE:xa_missing_entry>")
add_dependencies(xa_tests xa_fixture xa_bad_abi xa_bad_size xa_missing_callback xa_exception_fixture xa_missing_entry)
add_test(NAME assembly-contracts COMMAND xa_tests)
set_tests_properties(assembly-contracts PROPERTIES TIMEOUT 30)
add_test(NAME scalar-demo COMMAND xa_demo $<TARGET_FILE:xa_scalar>)
