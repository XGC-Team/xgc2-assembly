#ifndef XA_TEST_FIXTURE_H
#define XA_TEST_FIXTURE_H
#include "xgc2/assembly/abi.h"
#define FIX_PRODUCE 1u
#define FIX_FORWARD 2u
#define FIX_FAIL 3u
#define FIX_BAD_SCHEMA 4u
#define FIX_RETAIN 5u
#define FIX_FSM 6u
#define FIX_CREATE_FAIL 7u
#define FIX_START_FAIL 8u
#define FIX_STOP_FAIL 9u
#define FIX_NULL_INSTANCE 10u
#define FIX_IGNORE_ERROR 11u
#define FIX_THROW 12u
#define FIX_FOREIGN_THREAD 13u
#define FIX_BAD_TOKEN 14u
#define FIX_RETAIN_MUTABLE 15u
#define FIX_BUSY 16u
#define FIX_EMIT_EVERY 1u
#define FIX_EMIT_FIRST 2u
#define FIX_EMIT_EVENTS 3u
#define FIX_SCHEMA 1u
#define FIX_REPORT_SCHEMA 99u

typedef struct fixture_config {
    uint64_t mode, value, input, output, output2, report, target, emit_mode;
    uint64_t schema, allocation_size;
    void* probe_context;
    void (*probe)(void*, uint32_t, const xa_frame*);
} fixture_config;
typedef struct fixture_report {
    uint64_t invocation, now, elapsed, missed, reasons, input_flags;
    uint64_t input_value, input_version, input_address, retained_value;
    uint64_t events, first_event, last_event, event_source, correlation, generation;
    uint64_t state, foreign_status;
} fixture_report;
#endif
