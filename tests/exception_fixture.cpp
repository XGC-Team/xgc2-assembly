#include "fixture.h"
#include <cstring>
#include <stdexcept>
#include <thread>
struct Instance { const xa_host_api* host; fixture_config config; };
static xa_status create(const xa_host_api* h, const void* config, uint64_t size, void** out) {
    if (!out || !config || size != sizeof(fixture_config)) return XA_INVALID;
    auto* s = new Instance; s->host = h; std::memcpy(&s->config, config, sizeof(s->config)); *out = s; return XA_OK;
}
static xa_status start(void*) { return XA_OK; }
static xa_status step(void* ptr, const xa_frame* f) {
    auto& s = *static_cast<Instance*>(ptr);
    if (s.config.mode == FIX_THROW) throw std::runtime_error("fixture failure");
    xa_status status = XA_OK;
    std::thread invalid_caller([&] { xa_mutable_buffer b{}; status = s.host->allocate(s.host->context, 1, 8, 8, &b); });
    invalid_caller.join();
    fixture_report report{}; report.foreign_status = static_cast<uint64_t>(status);
    xa_mutable_buffer b{};
    status = s.host->allocate(s.host->context, FIX_REPORT_SCHEMA, sizeof(report), 8, &b);
    if (status != XA_OK) return status;
    std::memcpy(b.data, &report, sizeof(report));
    return s.host->publish(s.host->context, s.config.report, b.token, f->now_ns, 0);
}
static xa_status stop(void*) { return XA_OK; }
static void destroy(void* ptr) { delete static_cast<Instance*>(ptr); }
static const xa_plugin_api api{sizeof(xa_plugin_api), XA_ABI_VERSION, "exception-fixture", create, start, step, stop, destroy};
extern "C" XA_EXPORT const xa_plugin_api* xa_plugin_query() { return &api; }
