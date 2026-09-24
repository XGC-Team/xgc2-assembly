#include "fixture.h"
#include <stdlib.h>
#include <string.h>

typedef struct fixture {
    const xa_host_api* host;
    fixture_config config;
    uint64_t count, retained, state;
} fixture;
static void probe(fixture* s, uint32_t event, const xa_frame* frame) {
    if (s->config.probe) s->config.probe(s->config.probe_context, event, frame);
}
static xa_status create(const xa_host_api* host, const void* data, uint64_t size, void** out) {
    if (!out || !host || host->abi_version != XA_ABI_VERSION || host->struct_size != sizeof(*host) ||
        !data || size != sizeof(fixture_config)) return XA_INVALID;
    *out = NULL;
    fixture* s = (fixture*)calloc(1, sizeof(*s));
    if (!s) return XA_LIMIT;
    s->host = host; memcpy(&s->config, data, sizeof(s->config));
    probe(s, 1, NULL);
    if (s->config.mode == FIX_NULL_INSTANCE) { free(s); return XA_OK; }
    *out = s;
    return s->config.mode == FIX_CREATE_FAIL ? XA_PLUGIN_ERROR : XA_OK;
}
static xa_status start(void* instance) {
    fixture* s = (fixture*)instance; probe(s, 2, NULL);
    return s->config.mode == FIX_START_FAIL ? XA_PLUGIN_ERROR : XA_OK;
}
static xa_status publish_value(fixture* s, uint64_t output, uint64_t value, const xa_frame* f, uint64_t* token) {
    xa_mutable_buffer b = {0};
    const uint64_t schema = s->config.schema ? s->config.schema : FIX_SCHEMA;
    const uint64_t size = s->config.allocation_size ? s->config.allocation_size : sizeof(value);
    xa_status status = s->host->allocate(s->host->context, schema, size, 64, &b);
    if (status != XA_OK) return status;
    if (size >= sizeof(value)) memcpy(b.data, &value, sizeof(value));
    status = s->host->publish(s->host->context, output, b.token, f->now_ns, 0);
    *token = b.token;
    return status;
}
static xa_status step(void* instance, const xa_frame* f) {
    fixture* s = (fixture*)instance;
    const xa_host_api* h = s->host;
    fixture_report report = {0};
    report.invocation = f->invocation; report.now = f->now_ns; report.elapsed = f->elapsed_ns;
    report.missed = f->missed_periods; report.reasons = f->reasons;
    probe(s, 3, f);
    ++s->count;
    const xa_input* in = NULL;
    for (uint32_t i = 0; i < f->input_count; ++i)
        if (f->inputs[i].channel == s->config.input) in = &f->inputs[i];
    if (in) {
        report.input_flags = in->flags; report.input_version = in->version;
        report.input_address = (uint64_t)(uintptr_t)in->data;
        if (in->data && in->size >= sizeof(uint64_t)) memcpy(&report.input_value, in->data, sizeof(uint64_t));
    }
    report.events = f->event_count;
    if (f->event_count) {
        report.first_event = f->events[0].value[0];
        report.last_event = f->events[f->event_count - 1].value[0];
        report.event_source = f->events[0].source;
        report.correlation = f->events[0].correlation;
        report.generation = f->events[0].generation;
    }
    if (s->config.mode == FIX_FSM) {
        for (uint32_t i = 0; i < f->event_count; ++i) {
            if (f->events[i].kind == 1) s->state = 1;
            if (f->events[i].kind == 2) s->state = 0;
        }
    }
    report.state = s->state;
    if (s->config.mode == FIX_BAD_TOKEN) return h->publish(h->context, s->config.output, UINT64_MAX, f->now_ns, 0);
    if (s->config.mode == FIX_IGNORE_ERROR) (void)h->publish(h->context, UINT64_MAX, 0, f->now_ns, 0);
    if (s->config.mode == FIX_RETAIN_MUTABLE) {
        xa_mutable_buffer b = {0}; uint64_t retained = 0;
        xa_status rc = h->allocate(h->context, FIX_SCHEMA, 8, 8, &b);
        if (rc != XA_OK) return rc;
        return h->retain(h->context, b.token, &retained);
    }
    if (s->config.mode == FIX_RETAIN && in && in->token) {
        if (!s->retained) {
            xa_status rc = h->retain(h->context, in->token, &s->retained);
            if (rc != XA_OK) return rc;
        }
        const void* data = NULL; uint64_t size = 0;
        xa_status rc = h->view(h->context, s->retained, &data, &size);
        if (rc != XA_OK) return rc;
        if (size >= sizeof(uint64_t)) memcpy(&report.retained_value, data, sizeof(uint64_t));
        if (s->count == 2) {
            rc = h->release(h->context, s->retained);
            s->retained = 0;
            if (rc != XA_OK) return rc;
        }
    }
    if (s->config.output) {
        uint64_t token = 0;
        xa_status rc = XA_OK;
        if (s->config.mode == FIX_FORWARD) {
            if (in && (in->flags & XA_INPUT_VALID)) {
                token = in->token;
                rc = h->publish(h->context, s->config.output, token, in->timestamp_ns, in->valid_until_ns);
            }
        } else {
            if (s->config.mode == FIX_BAD_SCHEMA) s->config.schema = 888;
            rc = publish_value(s, s->config.output, s->config.value + s->count, f, &token);
        }
        if (rc != XA_OK) return rc;
        if (s->config.output2 && token) {
            rc = h->publish(h->context, s->config.output2, token, f->now_ns, 0);
            if (rc != XA_OK) return rc;
        }
    }
    if (s->config.target && (s->config.emit_mode == FIX_EMIT_EVERY ||
        (s->config.emit_mode == FIX_EMIT_FIRST && s->count == 1) ||
        (s->config.emit_mode == FIX_EMIT_EVENTS && f->event_count))) {
        xa_event e = {1, 999, 123, 7, {s->count, 0}};
        xa_status rc = h->emit(h->context, s->config.target, &e);
        if (rc != XA_OK) return rc;
    }
    if (s->config.report) {
        xa_mutable_buffer b = {0};
        xa_status rc = h->allocate(h->context, FIX_REPORT_SCHEMA, sizeof(report), 8, &b);
        if (rc != XA_OK) return rc;
        memcpy(b.data, &report, sizeof(report));
        rc = h->publish(h->context, s->config.report, b.token, f->now_ns, 0);
        if (rc != XA_OK) return rc;
    }
    if (s->config.mode == FIX_BUSY) return XA_BUSY;
    return s->config.mode == FIX_FAIL ? XA_PLUGIN_ERROR : XA_OK;
}
static xa_status stop(void* instance) {
    fixture* s = (fixture*)instance; probe(s, 4, NULL);
    return s->config.mode == FIX_STOP_FAIL ? XA_PLUGIN_ERROR : XA_OK;
}
static void destroy(void* instance) { fixture* s = (fixture*)instance; probe(s, 5, NULL); free(s); }
#ifndef BAD_ABI
#define BAD_ABI XA_ABI_VERSION
#endif
#ifndef BAD_SIZE
#define BAD_SIZE sizeof(xa_plugin_api)
#endif
#ifndef BAD_STEP
#define BAD_STEP step
#endif
static const xa_plugin_api api = {BAD_SIZE, BAD_ABI, "fixture", create, start, BAD_STEP, stop, destroy};
XA_EXPORT const xa_plugin_api* xa_plugin_query(void) { return &api; }
