#ifndef XGC2_ASSEMBLY_ABI_H
#define XGC2_ASSEMBLY_ABI_H
#include <stdint.h>
#ifdef __cplusplus
extern "C" {
#endif

#define XA_ABI_VERSION 1u
#if defined(__GNUC__)
#define XA_EXPORT __attribute__((visibility("default")))
#else
#define XA_EXPORT
#endif

typedef int32_t xa_status;
#define XA_OK 0
#define XA_INVALID 1
#define XA_NOT_FOUND 2
#define XA_LIMIT 3
#define XA_LIFECYCLE 4
#define XA_WRONG_THREAD 5
#define XA_PLUGIN_ERROR 6
#define XA_ABI_MISMATCH 7
#define XA_BUSY 8

#define XA_REASON_FIRST 1u
#define XA_REASON_PERIODIC 2u
#define XA_REASON_INPUT 4u
#define XA_REASON_EVENT 8u
#define XA_REASON_VALIDITY 16u
#define XA_INPUT_PRESENT 1u
#define XA_INPUT_VALID 2u

/* Event payloads are small values. Large data belongs in buffer channels. */
typedef struct xa_event {
    uint64_t kind;
    uint64_t source;
    uint64_t correlation;
    uint64_t generation;
    uint64_t value[2];
} xa_event;

typedef struct xa_input {
    uint64_t channel;
    uint64_t schema;
    uint64_t version;
    uint64_t timestamp_ns;
    uint64_t valid_until_ns; /* zero means no publisher-supplied expiry */
    uint64_t token;
    const void* data;
    uint64_t size;
    uint32_t flags;
    uint32_t reserved;
} xa_input;

typedef struct xa_mutable_buffer {
    uint64_t token;
    void* data;
    uint64_t size;
} xa_mutable_buffer;

typedef struct xa_frame {
    uint32_t struct_size;
    uint32_t reasons;
    uint64_t component;
    uint64_t now_ns;
    uint64_t elapsed_ns;
    uint64_t invocation;
    uint64_t missed_periods;
    const xa_input* inputs;
    uint32_t input_count;
    uint32_t event_count;
    const xa_event* events;
} xa_frame;

/* Services are callable only on the owner thread, during step(). All pointers
 * in a frame are callback-scoped. A retained token keeps a read-only buffer
 * alive between steps; release it explicitly. No exceptions cross this ABI.
 * Components must not create scheduler threads or call services asynchronously.
 */
typedef struct xa_host_api {
    uint32_t struct_size;
    uint32_t abi_version;
    void* context;
    xa_status (*allocate)(void*, uint64_t schema, uint64_t size,
                          uint32_t alignment, xa_mutable_buffer*);
    /* Publishing seals the buffer; the writable pointer must not be used again.
     * Input tokens can be published without copying their payload. Writes and
     * emitted events commit together only after a successful step. */
    xa_status (*publish)(void*, uint64_t channel, uint64_t token,
                         uint64_t timestamp_ns, uint64_t valid_until_ns);
    xa_status (*retain)(void*, uint64_t token, uint64_t* retained_token);
    xa_status (*release)(void*, uint64_t retained_token);
    xa_status (*view)(void*, uint64_t retained_token, const void** data,
                      uint64_t* size);
    xa_status (*emit)(void*, uint64_t target, const xa_event*);
} xa_host_api;

typedef struct xa_plugin_api {
    uint32_t struct_size;
    uint32_t abi_version;
    const char* name;
    xa_status (*create)(const xa_host_api*, const void* config,
                        uint64_t config_size, void** instance);
    xa_status (*start)(void* instance);
    xa_status (*step)(void* instance, const xa_frame*);
    xa_status (*stop)(void* instance);
    void (*destroy)(void* instance);
} xa_plugin_api;

/* The table is immutable and remains valid until the shared object is unloaded.
 * The v1 layout is frozen; incompatible changes require a new ABI version.
 * No C++ objects, allocators or exceptions are exchanged across the boundary.
 */
typedef const xa_plugin_api* (*xa_plugin_query_fn)(void);
XA_EXPORT const xa_plugin_api* xa_plugin_query(void);

#ifdef __cplusplus
}
#endif
#endif
