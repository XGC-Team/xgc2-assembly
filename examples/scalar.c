#include "xgc2/assembly/abi.h"
#include <stdlib.h>
#include <string.h>

typedef struct scalar { const xa_host_api* host; uint64_t channel; uint64_t value; } scalar;
static xa_status create(const xa_host_api* host, const void* config, uint64_t size, void** out) {
    if (!host || host->abi_version != XA_ABI_VERSION || host->struct_size != sizeof(*host) ||
        !out || !config || size != sizeof(uint64_t)) return XA_INVALID;
    *out = NULL;
    scalar* s = (scalar*)calloc(1, sizeof(*s));
    if (!s) return XA_LIMIT;
    s->host = host;
    memcpy(&s->channel, config, sizeof(s->channel));
    *out = s;
    return XA_OK;
}
static xa_status start(void* instance) { return instance ? XA_OK : XA_INVALID; }
static xa_status step(void* instance, const xa_frame* frame) {
    scalar* s = (scalar*)instance;
    xa_mutable_buffer buffer = {0};
    xa_status status = s->host->allocate(s->host->context, 1, sizeof(uint64_t), 8, &buffer);
    if (status != XA_OK) return status;
    ++s->value;
    memcpy(buffer.data, &s->value, sizeof(s->value));
    return s->host->publish(s->host->context, s->channel, buffer.token, frame->now_ns, 0);
}
static xa_status stop(void* instance) { return instance ? XA_OK : XA_INVALID; }
static void destroy(void* instance) { free(instance); }
static const xa_plugin_api api = {sizeof(xa_plugin_api), XA_ABI_VERSION, "scalar", create, start, step, stop, destroy};
XA_EXPORT const xa_plugin_api* xa_plugin_query(void) { return &api; }
