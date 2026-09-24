#include "xgc2/assembly/assembly.hpp"
#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <dlfcn.h>
#include <filesystem>
#include <limits>
#include <map>
#include <mutex>
#include <set>
#include <stdexcept>
#include <thread>
#include <utility>

namespace xgc2::assembly {
namespace {
Status error(xa_status code, const char* text) { return {code, text}; }
constexpr auto max_time = static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max());
struct Budget {
    std::mutex mutex;
    std::size_t bytes{0}, count{0}, max_bytes, max_count;
    explicit Budget(const Limits& l) : max_bytes(l.buffer_bytes), max_count(l.buffers) {}
};
struct Ring {
    std::vector<xa_event> storage;
    std::size_t head{0}, count{0};
    bool closed{false};
    explicit Ring(std::size_t capacity) : storage(capacity) {}
    bool full(std::size_t extra) const { return extra > storage.size() - count; }
    void push(const xa_event& event) noexcept {
        storage[(head + count) % storage.size()] = event;
        ++count;
    }
    std::vector<xa_event> take(std::size_t n) {
        n = std::min(n, count);
        std::vector<xa_event> result;
        result.reserve(n);
        for (std::size_t i = 0; i < n; ++i) result.push_back(storage[(head + i) % storage.size()]);
        head = (head + n) % storage.size();
        count -= n;
        return result;
    }
    void close() noexcept { closed = true; count = 0; }
};
struct Library {
    void* handle{nullptr};
    xa_plugin_api api{};
    ~Library() { if (handle) dlclose(handle); }
    Status open(const std::string& path) {
        if (!std::filesystem::path(path).is_absolute()) return error(XA_INVALID, "plugin path must be absolute");
        std::error_code ec;
        const auto canonical = std::filesystem::canonical(path, ec);
        if (ec || !std::filesystem::is_regular_file(canonical, ec)) return error(XA_NOT_FOUND, "plugin file not found");
        handle = dlopen(canonical.c_str(), RTLD_NOW | RTLD_LOCAL);
        if (!handle) {
            const char* text = dlerror();
            return {XA_NOT_FOUND, text ? text : "dlopen failed"};
        }
        dlerror();
        void* symbol = dlsym(handle, "xa_plugin_query");
        const char* text = dlerror();
        if (text || !symbol) return error(XA_ABI_MISMATCH, "plugin entry point missing");
        xa_plugin_query_fn query = nullptr;
        static_assert(sizeof(query) == sizeof(symbol), "Linux function pointer size");
        std::memcpy(&query, &symbol, sizeof(query));
        const xa_plugin_api* table = nullptr;
        try { table = query(); } catch (...) { return error(XA_PLUGIN_ERROR, "plugin query threw"); }
        if (!table || table->struct_size != sizeof(xa_plugin_api) || table->abi_version != XA_ABI_VERSION)
            return error(XA_ABI_MISMATCH, "plugin ABI mismatch");
        api = *table;
        if (!api.name || !api.create || !api.start || !api.step || !api.stop || !api.destroy)
            return error(XA_ABI_MISMATCH, "plugin callback table incomplete");
        return {};
    }
};
template<class Fn> xa_status guarded(Fn&& fn) noexcept {
    try { return fn(); } catch (...) { return XA_PLUGIN_ERROR; }
}
thread_local void* active_component = nullptr;
thread_local void* active_domain = nullptr;
struct ExecutionScope {
    explicit ExecutionScope(void* domain) { active_domain = domain; }
    ~ExecutionScope() { active_domain = nullptr; }
};
struct CallbackScope {
    void* previous;
    explicit CallbackScope(void* component) : previous(active_component) { active_component = component; }
    ~CallbackScope() { active_component = previous; }
};
} // namespace

class Buffer {
public:
    std::uint64_t schema;
    std::size_t size;
    void* data;
    std::shared_ptr<Budget> budget;
    Buffer(std::uint64_t s, std::size_t n, void* p, std::shared_ptr<Budget> b)
        : schema(s), size(n), data(p), budget(std::move(b)) {}
    ~Buffer() {
        std::free(data);
        std::lock_guard<std::mutex> lock(budget->mutex);
        budget->bytes -= size;
        --budget->count;
    }
};
namespace {
std::shared_ptr<Buffer> allocate_buffer(const std::shared_ptr<Budget>& budget, std::uint64_t schema,
                                      std::size_t size, std::size_t alignment) {
    std::lock_guard<std::mutex> lock(budget->mutex);
    if (!size || size > budget->max_bytes - budget->bytes || budget->count >= budget->max_count) return {};
    void* data = nullptr;
    if (posix_memalign(&data, alignment, size) != 0) return {};
    std::shared_ptr<Buffer> result;
    try { result = std::make_shared<Buffer>(schema, size, data, budget); }
    catch (...) { std::free(data); return {}; }
    budget->bytes += size;
    ++budget->count;
    return result;
}
struct Value {
    std::shared_ptr<Buffer> buffer;
    std::uint64_t version{0}, timestamp{0}, expiry{0};
};
struct Pending { std::shared_ptr<Buffer> buffer; std::uint64_t timestamp, expiry; };
struct Emission { std::uint64_t target; xa_event event; };
} // namespace

struct Assembly::Shared {
    struct Channel { std::uint64_t schema, writer{0}; Value value; };
    Limits limits;
    std::shared_ptr<Budget> budget;
    mutable std::mutex mutex;
    bool sealed{false};
    std::uint64_t version{0};
    std::map<std::uint64_t, Channel> channels;
    std::map<std::uint64_t, Ring> inboxes;
    std::set<std::string> domain_names;
    explicit Shared(Limits l) : limits(l), budget(std::make_shared<Budget>(l)) {}
    Status commit(std::uint64_t writer, const std::map<std::uint64_t, Pending>& writes,
                  const std::vector<Emission>& emissions) {
        std::lock_guard<std::mutex> lock(mutex);
        if (!writes.empty() && version == std::numeric_limits<std::uint64_t>::max())
            return error(XA_LIMIT, "version space exhausted");
        std::map<std::uint64_t, std::size_t> counts;
        for (const auto& e : emissions) ++counts[e.target];
        for (const auto& [id, n] : counts) {
            auto it = inboxes.find(id);
            if (it == inboxes.end()) return error(XA_NOT_FOUND, "event target missing");
            if (it->second.closed) return error(XA_LIFECYCLE, "event target closed");
            if (it->second.full(n)) return error(XA_LIMIT, "event mailbox full");
        }
        for (const auto& [id, p] : writes) {
            const auto it = channels.find(id);
            if (it == channels.end() || it->second.writer != writer || it->second.schema != p.buffer->schema)
                return error(XA_INVALID, "output ownership or schema mismatch");
        }
        // Validation above may allocate; everything below is non-allocating.
        if (!writes.empty()) ++version;
        for (const auto& [id, p] : writes) channels.at(id).value = {p.buffer, version, p.timestamp, p.expiry};
        for (const auto& e : emissions) inboxes.at(e.target).push(e.event);
        return {};
    }
};

struct Domain::Impl {
    struct Component {
        struct Token { std::shared_ptr<Buffer> buffer; bool writable; bool persistent; };
        std::unique_ptr<Library> library; // outlives the instance and all callbacks
        ComponentSpec spec;
        std::shared_ptr<Assembly::Shared> shared;
        void* instance{nullptr};
        bool start_attempted{false};
        xa_host_api host{};
        ComponentStats stats;
        std::map<std::uint64_t, std::uint64_t> output_schemas;
        std::map<std::uint64_t, Token> tokens;
        std::uint64_t next_token{1}, next_due{0}, last_invocation{0}, now{0};
        std::vector<std::pair<std::uint64_t, bool>> seen;
        std::map<std::uint64_t, Pending> writes;
        std::vector<Emission> emissions;
        xa_status service_error{XA_OK};
        explicit Component(ComponentSpec s, std::shared_ptr<Assembly::Shared> bus)
            : library(std::make_unique<Library>()), spec(std::move(s)), shared(std::move(bus)), seen(spec.inputs.size()) {
            stats.id = spec.id;
            emissions.reserve(shared->limits.emissions_per_step);
            host = {sizeof(xa_host_api), XA_ABI_VERSION, this, allocate, publish, retain, release, view, emit};
        }
        template<class Fn> static xa_status service(void* context, Fn&& fn) noexcept {
            if (!context || active_component != context) return XA_WRONG_THREAD;
            auto& c = *static_cast<Component*>(context);
            xa_status result = XA_OK;
            try { result = fn(c); } catch (...) { result = XA_LIMIT; }
            if (result != XA_OK && c.service_error == XA_OK) c.service_error = result;
            return result;
        }
        std::uint64_t token(std::shared_ptr<Buffer> buffer, bool writable, bool persistent) {
            if (tokens.size() >= shared->limits.tokens_per_component || next_token == std::numeric_limits<std::uint64_t>::max())
                return 0;
            const auto id = next_token++;
            tokens.emplace(id, Token{std::move(buffer), writable, persistent});
            return id;
        }
        static xa_status allocate(void* p, std::uint64_t schema, std::uint64_t size,
                                  std::uint32_t alignment, xa_mutable_buffer* out) noexcept {
            return service(p, [&](Component& c) -> xa_status {
                if (!out) return XA_INVALID;
                *out = {};
                if (!schema || !size || size > std::numeric_limits<std::size_t>::max() ||
                    alignment < sizeof(void*) || alignment > 4096 || (alignment & (alignment - 1)) != 0) return XA_INVALID;
                if (c.tokens.size() >= c.shared->limits.tokens_per_component) return XA_LIMIT;
                auto buffer = allocate_buffer(c.shared->budget, schema, static_cast<std::size_t>(size), alignment);
                if (!buffer) return XA_LIMIT;
                const auto id = c.token(buffer, true, false);
                if (!id) return XA_LIMIT;
                *out = {id, buffer->data, size};
                return XA_OK;
            });
        }
        static xa_status publish(void* p, std::uint64_t channel, std::uint64_t id,
                                 std::uint64_t timestamp, std::uint64_t expiry) noexcept {
            return service(p, [&](Component& c) -> xa_status {
                auto t = c.tokens.find(id);
                auto out = c.output_schemas.find(channel);
                if (t == c.tokens.end() || out == c.output_schemas.end()) return XA_INVALID;
                if (out->second != t->second.buffer->schema || timestamp > c.now ||
                    (expiry && expiry < timestamp) || c.writes.count(channel)) return XA_INVALID;
                c.writes.emplace(channel, Pending{t->second.buffer, timestamp, expiry});
                t->second.writable = false;
                return XA_OK;
            });
        }
        static xa_status retain(void* p, std::uint64_t id, std::uint64_t* out) noexcept {
            return service(p, [&](Component& c) -> xa_status {
                if (!out) return XA_INVALID;
                *out = 0;
                const auto t = c.tokens.find(id);
                if (t == c.tokens.end() || t->second.writable) return XA_INVALID;
                *out = c.token(t->second.buffer, false, true);
                return *out ? XA_OK : XA_LIMIT;
            });
        }
        static xa_status release(void* p, std::uint64_t id) noexcept {
            return service(p, [&](Component& c) -> xa_status {
                const auto t = c.tokens.find(id);
                if (t == c.tokens.end() || !t->second.persistent) return XA_INVALID;
                c.tokens.erase(t);
                return XA_OK;
            });
        }
        static xa_status view(void* p, std::uint64_t id, const void** data, std::uint64_t* size) noexcept {
            return service(p, [&](Component& c) -> xa_status {
                if (!data || !size) return XA_INVALID;
                *data = nullptr; *size = 0;
                const auto t = c.tokens.find(id);
                if (t == c.tokens.end() || t->second.writable) return XA_INVALID;
                *data = t->second.buffer->data;
                *size = t->second.buffer->size;
                return XA_OK;
            });
        }
        static xa_status emit(void* p, std::uint64_t target, const xa_event* event) noexcept {
            return service(p, [&](Component& c) -> xa_status {
                if (!event || !event->kind || !target) return XA_INVALID;
                if (c.emissions.size() >= c.shared->limits.emissions_per_step) return XA_LIMIT;
                {
                    std::lock_guard<std::mutex> lock(c.shared->mutex);
                    const auto it = c.shared->inboxes.find(target);
                    if (it == c.shared->inboxes.end()) return XA_NOT_FOUND;
                    if (it->second.closed) return XA_LIFECYCLE;
                }
                auto copy = *event;
                copy.source = c.spec.id;
                c.emissions.push_back({target, copy});
                return XA_OK;
            });
        }
        void clear_transient() {
            writes.clear(); emissions.clear();
            for (auto it = tokens.begin(); it != tokens.end();) {
                if (!it->second.persistent) it = tokens.erase(it); else ++it;
            }
            service_error = XA_OK;
        }
        xa_status cleanup() noexcept {
            xa_status result = XA_OK;
            if (instance) {
                if (start_attempted) result = guarded([&] { return library->api.stop(instance); });
                try { library->api.destroy(instance); } catch (...) { result = XA_PLUGIN_ERROR; }
                instance = nullptr;
            }
            tokens.clear(); writes.clear(); emissions.clear();
            if (stats.lifecycle != Lifecycle::faulted) stats.lifecycle = Lifecycle::stopped;
            return result;
        }
        void fault(xa_status code) {
            stats.lifecycle = Lifecycle::faulted; stats.last_error = code; ++stats.failures;
            std::lock_guard<std::mutex> lock(shared->mutex);
            shared->inboxes.at(spec.id).close();
        }
    };
    std::shared_ptr<Assembly::Shared> shared;
    std::string name;
    mutable std::mutex execution;
    std::thread::id owner;
    bool started{false}, finished{false};
    std::uint64_t last_step{0};
    std::vector<std::unique_ptr<Component>> components;
    Impl(std::shared_ptr<Assembly::Shared> bus, std::string n) : shared(std::move(bus)), name(std::move(n)) {}
    xa_status cleanup() noexcept {
        {
            std::lock_guard<std::mutex> lock(shared->mutex);
            for (const auto& c : components) shared->inboxes.at(c->spec.id).close();
        }
        xa_status result = XA_OK;
        for (auto it = components.rbegin(); it != components.rend(); ++it) {
            const auto status = (*it)->cleanup();
            if (status != XA_OK && result == XA_OK) result = status;
        }
        finished = true; started = false;
        return result;
    }
};

Assembly::Assembly(Limits limits) {
    if (!limits.channels || !limits.components || !limits.mailbox_events || !limits.events_per_step ||
        !limits.buffers || !limits.buffer_bytes || !limits.tokens_per_component || !limits.emissions_per_step)
        throw std::invalid_argument("limits must be positive");
    if (limits.events_per_step > UINT32_MAX)
        throw std::invalid_argument("event batch exceeds ABI count range");
    shared_ = std::make_shared<Shared>(limits);
}
Assembly::~Assembly() = default;
Status Assembly::channel(std::uint64_t id, std::uint64_t schema) {
    std::lock_guard<std::mutex> lock(shared_->mutex);
    if (shared_->sealed) return error(XA_LIFECYCLE, "topology is sealed");
    if (!id || !schema || shared_->channels.count(id)) return error(XA_INVALID, "invalid or duplicate channel");
    if (shared_->channels.size() >= shared_->limits.channels) return error(XA_LIMIT, "channel limit");
    shared_->channels.emplace(id, Shared::Channel{schema, 0, {}});
    return {};
}
std::shared_ptr<Domain> Assembly::domain(std::string name) {
    std::lock_guard<std::mutex> lock(shared_->mutex);
    if (shared_->sealed || name.empty() || shared_->domain_names.count(name)) throw std::logic_error("invalid domain or sealed topology");
    auto result = std::shared_ptr<Domain>(new Domain(shared_, name));
    shared_->domain_names.insert(std::move(name));
    return result;
}
Status Assembly::seal() {
    std::lock_guard<std::mutex> lock(shared_->mutex);
    if (shared_->sealed) return {};
    if (shared_->inboxes.empty()) return error(XA_INVALID, "no components");
    shared_->sealed = true;
    return {};
}
Status Assembly::post(std::uint64_t target, xa_event event) {
    std::lock_guard<std::mutex> lock(shared_->mutex);
    if (!shared_->sealed) return error(XA_LIFECYCLE, "topology is not sealed");
    if (!event.kind) return error(XA_INVALID, "event kind is zero");
    auto it = shared_->inboxes.find(target);
    if (it == shared_->inboxes.end()) return error(XA_NOT_FOUND, "event target missing");
    if (it->second.closed) return error(XA_LIFECYCLE, "event target closed");
    if (it->second.full(1)) return error(XA_LIMIT, "event mailbox full");
    event.source = 0;
    it->second.push(event);
    return {};
}
Status Assembly::publish_copy(std::uint64_t channel_id, const void* data, std::size_t size,
                              std::uint64_t timestamp, std::uint64_t expiry) {
    if (!data || !size || timestamp > max_time || (expiry && expiry < timestamp)) return error(XA_INVALID, "invalid external sample");
    std::uint64_t schema = 0;
    {
        std::lock_guard<std::mutex> lock(shared_->mutex);
        if (!shared_->sealed) return error(XA_LIFECYCLE, "topology is not sealed");
        auto it = shared_->channels.find(channel_id);
        if (it == shared_->channels.end()) return error(XA_NOT_FOUND, "channel missing");
        if (it->second.writer != 0) return error(XA_INVALID, "channel has a component writer");
        schema = it->second.schema;
    }
    auto buffer = allocate_buffer(shared_->budget, schema, size, 64);
    if (!buffer) return error(XA_LIMIT, "buffer budget exhausted");
    std::memcpy(buffer->data, data, size);
    return shared_->commit(0, {{channel_id, Pending{buffer, timestamp, expiry}}}, {});
}
Sample Assembly::sample(std::uint64_t id) const {
    std::lock_guard<std::mutex> lock(shared_->mutex);
    Sample sample;
    auto it = shared_->channels.find(id);
    if (it == shared_->channels.end()) return sample;
    const auto& value = it->second.value;
    sample.buffer_ = value.buffer; sample.schema = it->second.schema;
    sample.version = value.version; sample.timestamp_ns = value.timestamp; sample.valid_until_ns = value.expiry;
    return sample;
}
std::size_t Assembly::live_buffer_bytes() const {
    std::lock_guard<std::mutex> lock(shared_->budget->mutex);
    return shared_->budget->bytes;
}
const void* Sample::data() const noexcept { return buffer_ ? buffer_->data : nullptr; }
std::size_t Sample::size() const noexcept { return buffer_ ? buffer_->size : 0; }
Sample::operator bool() const noexcept { return static_cast<bool>(buffer_); }

Domain::Domain(std::shared_ptr<Assembly::Shared> bus, std::string name)
    : impl_(std::make_unique<Impl>(std::move(bus), std::move(name))) {}
Domain::~Domain() {
    if (impl_->started) {
        // A live owner-thread object cannot safely migrate during destruction.
        if (impl_->owner != std::this_thread::get_id() || active_domain) std::terminate();
        (void)stop();
    }
}
const std::string& Domain::name() const noexcept { return impl_->name; }
Status Domain::add(ComponentSpec spec) {
    auto& d = *impl_;
    if (active_domain) return error(XA_BUSY, "nested domain execution is not allowed");
    std::scoped_lock lock(d.execution, d.shared->mutex);
    ExecutionScope execution(&d);
    if (d.shared->sealed) return error(XA_LIFECYCLE, "topology is sealed");
    if (!spec.id || d.shared->inboxes.count(spec.id)) return error(XA_INVALID, "invalid or duplicate component id");
    if (d.shared->inboxes.size() >= d.shared->limits.components) return error(XA_LIMIT, "component limit");
    if (spec.schedule.period_ns > max_time || spec.schedule.phase_ns > max_time ||
        (!spec.schedule.period_ns && spec.schedule.phase_ns) ||
        (!spec.schedule.period_ns && !spec.schedule.on_input && !spec.schedule.on_event))
        return error(XA_INVALID, "invalid schedule");
    if (spec.inputs.size() > UINT32_MAX || spec.inputs.size() > d.shared->limits.tokens_per_component)
        return error(XA_LIMIT, "input limit");
    std::set<std::uint64_t> inputs, outputs;
    for (const auto& p : spec.inputs)
        if (!d.shared->channels.count(p.channel) || !inputs.insert(p.channel).second)
            return error(XA_INVALID, "unknown or duplicate input");
    for (auto id : spec.outputs) {
        auto it = d.shared->channels.find(id);
        if (it == d.shared->channels.end() || it->second.writer || !outputs.insert(id).second)
            return error(XA_INVALID, "unknown output or duplicate writer");
    }
    auto c = std::make_unique<Impl::Component>(std::move(spec), d.shared);
    auto status = c->library->open(c->spec.library);
    if (!status) return status;
    for (auto id : c->spec.outputs) c->output_schemas[id] = d.shared->channels.at(id).schema;
    d.components.reserve(d.components.size() + 1);
    d.shared->inboxes.emplace(c->spec.id, Ring(d.shared->limits.mailbox_events));
    for (auto id : c->spec.outputs) d.shared->channels.at(id).writer = c->spec.id;
    d.components.push_back(std::move(c));
    return {};
}
Status Domain::start(std::uint64_t now) {
    auto& d = *impl_;
    // A normal mutex must not be try_locked recursively, even on its owner.
    if (active_domain) return error(XA_BUSY, "nested domain execution is not allowed");
    std::unique_lock<std::mutex> lock(d.execution, std::try_to_lock);
    if (!lock.owns_lock()) return error(XA_BUSY, "domain is executing");
    ExecutionScope execution(&d);
    if (d.started || d.finished) return error(XA_LIFECYCLE, "domain is not prepared");
    if (now > max_time) return error(XA_INVALID, "time out of range");
    {
        std::lock_guard<std::mutex> shared_lock(d.shared->mutex);
        if (!d.shared->sealed) return error(XA_LIFECYCLE, "topology is not sealed");
    }
    for (const auto& c : d.components)
        if (c->spec.schedule.phase_ns > max_time - now) return error(XA_INVALID, "phase overflows clock range");
    std::stable_sort(d.components.begin(), d.components.end(), [](const auto& a, const auto& b) { return a->spec.stage < b->spec.stage; });
    d.owner = std::this_thread::get_id(); d.last_step = now;
    for (auto& c : d.components) {
        c->next_due = now + c->spec.schedule.phase_ns;
        xa_status result = guarded([&] { return c->library->api.create(&c->host,
            c->spec.config.empty() ? nullptr : c->spec.config.data(), c->spec.config.size(), &c->instance); });
        if (result == XA_OK && !c->instance) result = XA_PLUGIN_ERROR;
        if (result == XA_OK) {
            c->start_attempted = true;
            result = guarded([&] { return c->library->api.start(c->instance); });
        }
        if (result != XA_OK) {
            c->fault(result); d.cleanup();
            return error(result, "component startup failed");
        }
        c->stats.lifecycle = Lifecycle::running;
    }
    d.started = true;
    return {};
}
Status Domain::step(std::uint64_t now) {
    auto& d = *impl_;
    // A normal mutex must not be try_locked recursively, even on its owner.
    if (active_domain) return error(XA_BUSY, "nested domain execution is not allowed");
    std::unique_lock<std::mutex> lock(d.execution, std::try_to_lock);
    if (!lock.owns_lock()) return error(XA_BUSY, "domain is executing");
    ExecutionScope execution(&d);
    if (!d.started) return error(XA_LIFECYCLE, "domain is not running");
    if (d.owner != std::this_thread::get_id()) return error(XA_WRONG_THREAD, "wrong domain owner");
    if (now > max_time || now < d.last_step) return error(XA_INVALID, "clock moved backwards or out of range");
    d.last_step = now;
    std::vector<std::size_t> boundary_events;
    {
        std::lock_guard<std::mutex> shared_lock(d.shared->mutex);
        for (const auto& c : d.components) boundary_events.push_back(d.shared->inboxes.at(c->spec.id).count);
    }
    std::map<std::uint64_t, Assembly::Shared::Channel> snapshot;
    std::uint32_t stage = 0;
    bool have_stage = false;
    Status result;
    for (std::size_t index = 0; index < d.components.size(); ++index) {
        auto& c = *d.components[index];
        if (c.stats.lifecycle != Lifecycle::running) continue;
        if (!have_stage || c.spec.stage != stage) {
            std::lock_guard<std::mutex> shared_lock(d.shared->mutex);
            snapshot = d.shared->channels;
            stage = c.spec.stage; have_stage = true;
        }
        std::vector<xa_input> inputs;
        std::vector<std::pair<std::uint64_t, bool>> seen;
        bool dirty = false, validity_changed = false;
        for (std::size_t i = 0; i < c.spec.inputs.size(); ++i) {
            const auto& port = c.spec.inputs[i];
            const auto& slot = snapshot.at(port.channel);
            const auto& v = slot.value;
            bool valid = v.buffer && v.timestamp <= now && (!v.expiry || now < v.expiry) &&
                (!port.max_age_ns || (v.timestamp <= now && now - v.timestamp <= port.max_age_ns));
            dirty = dirty || c.seen[i].first != v.version;
            validity_changed = validity_changed || c.seen[i].second != valid;
            seen.emplace_back(v.version, valid);
            inputs.push_back({port.channel, slot.schema, v.version, v.timestamp, v.expiry, 0,
                              v.buffer ? v.buffer->data : nullptr, v.buffer ? v.buffer->size : 0,
                              (v.buffer ? XA_INPUT_PRESENT : 0u) | (valid ? XA_INPUT_VALID : 0u), 0});
        }
        const bool first = c.stats.invocations == 0;
        const bool due = c.spec.schedule.period_ns && now >= c.next_due;
        const bool event_ready = boundary_events[index] != 0;
        if (!(due || (c.spec.schedule.on_input && (first || dirty || validity_changed)) ||
            (c.spec.schedule.on_event && event_ready))) continue;
        std::uint64_t missed = 0;
        if (due) {
            const auto period = c.spec.schedule.period_ns;
            missed = (now - c.next_due) / period;
            // UINT64_MAX is beyond the accepted clock range and means no further release.
            const auto steps = missed + 1;
            c.next_due = steps > (UINT64_MAX - c.next_due) / period ? UINT64_MAX : c.next_due + steps * period;
        }
        std::vector<xa_event> events;
        {
            std::lock_guard<std::mutex> shared_lock(d.shared->mutex);
            events = d.shared->inboxes.at(c.spec.id).take(std::min(boundary_events[index], d.shared->limits.events_per_step));
        }
        c.now = now;
        xa_status status = XA_OK;
        for (std::size_t i = 0; i < inputs.size(); ++i) {
            auto buffer = snapshot.at(inputs[i].channel).value.buffer;
            if (buffer) {
                inputs[i].token = c.token(buffer, false, false);
                if (!inputs[i].token) { status = XA_LIMIT; break; }
            }
        }
        const auto reasons = (first ? XA_REASON_FIRST : 0u) | (due ? XA_REASON_PERIODIC : 0u) |
            (dirty ? XA_REASON_INPUT : 0u) | (event_ready ? XA_REASON_EVENT : 0u) |
            (validity_changed ? XA_REASON_VALIDITY : 0u);
        const xa_frame frame{sizeof(xa_frame), reasons, c.spec.id, now, first ? 0 : now - c.last_invocation,
            c.stats.invocations + 1, missed, inputs.data(), static_cast<std::uint32_t>(inputs.size()),
            static_cast<std::uint32_t>(events.size()), events.data()};
        ++c.stats.invocations; c.stats.missed_periods += missed; c.last_invocation = now;
        if (status == XA_OK) {
            const auto begin = std::chrono::steady_clock::now();
            {
                CallbackScope scope(&c);
                status = guarded([&] { return c.library->api.step(c.instance, &frame); });
            }
            const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - begin).count();
            c.stats.callback_max_ns = std::max(c.stats.callback_max_ns, static_cast<std::uint64_t>(elapsed));
            if (status == XA_OK) status = c.service_error;
        }
        if (status == XA_OK) {
            try {
                const auto commit_status = d.shared->commit(c.spec.id, c.writes, c.emissions);
                status = commit_status.code;
                if (!commit_status) ++c.stats.rejected_transactions;
            } catch (...) { status = XA_LIMIT; ++c.stats.rejected_transactions; }
        }
        c.clear_transient();
        if (status != XA_OK) {
            c.fault(status);
            if (result) result = error(status, "component step or transaction failed");
        } else c.seen = std::move(seen);
    }
    return result;
}
Status Domain::stop() {
    auto& d = *impl_;
    // A normal mutex must not be try_locked recursively, even on its owner.
    if (active_domain) return error(XA_BUSY, "nested domain execution is not allowed");
    std::unique_lock<std::mutex> lock(d.execution, std::try_to_lock);
    if (!lock.owns_lock()) return error(XA_BUSY, "domain is executing");
    ExecutionScope execution(&d);
    if (d.finished) return {};
    if (!d.started) return error(XA_LIFECYCLE, "domain was not started");
    if (d.owner != std::this_thread::get_id()) return error(XA_WRONG_THREAD, "wrong domain owner");
    const auto result = d.cleanup();
    return result == XA_OK ? Status{} : error(result, "component cleanup failed");
}
std::vector<ComponentStats> Domain::stats() const {
    if (active_domain && active_domain != impl_.get())
        throw std::logic_error("cross-domain stats are not callable during execution");
    std::unique_lock<std::mutex> lock(impl_->execution, std::defer_lock);
    if (!active_domain) lock.lock();
    std::vector<ComponentStats> result;
    for (const auto& c : impl_->components) result.push_back(c->stats);
    return result;
}
} // namespace xgc2::assembly
