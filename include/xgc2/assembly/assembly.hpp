#pragma once
#include "xgc2/assembly/abi.h"
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace xgc2::assembly {
struct Status {
    xa_status code{XA_OK};
    std::string message;
    explicit operator bool() const noexcept { return code == XA_OK; }
};
struct Limits {
    std::size_t channels{256};
    std::size_t components{128};
    std::size_t mailbox_events{128};
    std::size_t events_per_step{32};
    std::size_t buffers{512};
    std::size_t buffer_bytes{256u * 1024u * 1024u};
    std::size_t tokens_per_component{128};
    std::size_t emissions_per_step{32};
};
struct Schedule {
    std::uint64_t period_ns{0};
    std::uint64_t phase_ns{0};
    bool on_input{false};
    bool on_event{true};
};
struct InputPort {
    std::uint64_t channel{0};
    std::uint64_t max_age_ns{0}; // zero disables the consumer-side age limit
};
struct ComponentSpec {
    std::uint64_t id{0}; // zero is reserved for external writers
    std::string library; // explicit absolute path; no directory scanning
    std::uint32_t stage{0};
    Schedule schedule;
    std::vector<InputPort> inputs;
    std::vector<std::uint64_t> outputs;
    std::vector<std::uint8_t> config;
};
enum class Lifecycle { prepared, running, faulted, stopped };
struct ComponentStats {
    std::uint64_t id{0};
    Lifecycle lifecycle{Lifecycle::prepared};
    std::uint64_t invocations{0};
    std::uint64_t missed_periods{0};
    std::uint64_t failures{0};
    std::uint64_t rejected_transactions{0};
    std::uint64_t callback_max_ns{0};
    xa_status last_error{XA_OK};
};
class Buffer;
class Sample {
public:
    std::uint64_t schema{0}, version{0}, timestamp_ns{0}, valid_until_ns{0};
    const void* data() const noexcept;
    std::size_t size() const noexcept;
    explicit operator bool() const noexcept;
private:
    std::shared_ptr<const Buffer> buffer_;
    friend class Assembly;
    friend class Domain;
};
class Domain;
class Assembly {
public:
    explicit Assembly(Limits limits = {});
    ~Assembly();
    Assembly(const Assembly&) = delete;
    Assembly& operator=(const Assembly&) = delete;
    Status channel(std::uint64_t id, std::uint64_t schema);
    std::shared_ptr<Domain> domain(std::string name);
    // Freeze topology before starting any domain. No online topology mutation.
    Status seal();
    Status post(std::uint64_t target, xa_event event);
    // One boundary copy for externally-owned input. Plugins allocate in-place.
    Status publish_copy(std::uint64_t channel, const void* data, std::size_t size,
                        std::uint64_t timestamp_ns, std::uint64_t valid_until_ns = 0);
    Sample sample(std::uint64_t channel) const;
    std::size_t live_buffer_bytes() const;
private:
    struct Shared;
    std::shared_ptr<Shared> shared_;
    friend class Domain;
};
class Domain {
public:
    ~Domain();
    Domain(const Domain&) = delete;
    Domain& operator=(const Domain&) = delete;
    Status add(ComponentSpec spec);
    // start/step/stop must be serialized on the same owner thread. Stop before
    // destruction. Nested calls (including into other domains) return XA_BUSY.
    // DomainThread provides an owning, joining driver.
    Status start(std::uint64_t now_ns);
    Status step(std::uint64_t now_ns);
    Status stop();
    // Cross-domain inspection from an executing callback throws logic_error.
    std::vector<ComponentStats> stats() const;
    const std::string& name() const noexcept;
private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
    Domain(std::shared_ptr<Assembly::Shared>, std::string);
    friend class Assembly;
};
// One thread per explicitly selected execution domain, never per plugin.
// Events and dirty inputs are evaluated at each bounded base-period boundary.
// stop() joins: it cannot forcibly interrupt an uncooperative native callback.
class DomainThread {
public:
    explicit DomainThread(std::shared_ptr<Domain> domain, std::uint64_t base_period_ns);
    ~DomainThread();
    DomainThread(const DomainThread&) = delete;
    DomainThread& operator=(const DomainThread&) = delete;
    Status start();
    // Nonjoining stop request is safe from the worker's callback.
    void request_stop() noexcept;
    // Joining calls from any driver callback return XA_BUSY to avoid cycles.
    Status stop();
    Status result() const;
private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};
} // namespace xgc2::assembly
