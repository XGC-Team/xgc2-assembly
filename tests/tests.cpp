#include "xgc2/assembly/assembly.hpp"
#include "fixture.h"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <functional>
#include <future>
#include <iostream>
#include <mutex>
#include <set>
#include <stdexcept>
#include <thread>
#include <vector>
using namespace xgc2::assembly;
using namespace std::chrono_literals;
#define CHECK(x) do { if (!(x)) throw std::runtime_error(std::string(__func__) + ": " + #x); } while (false)
#define OK(x) do { const auto s_ = (x); if (!s_) throw std::runtime_error(std::string(__func__) + ": " + #x + " -> " + s_.message); } while (false)
static ComponentSpec spec(std::uint64_t id, fixture_config config, Schedule schedule = {10, 0, false, false},
                          std::uint32_t stage = 0, const char* library = FIXTURE_PATH) {
    ComponentSpec s;
    s.id = id; s.library = library; s.stage = stage; s.schedule = schedule;
    if (config.input) s.inputs.push_back({config.input, 0});
    if (config.output) s.outputs.push_back(config.output);
    if (config.output2) s.outputs.push_back(config.output2);
    if (config.report) s.outputs.push_back(config.report);
    s.config.resize(sizeof(config)); std::memcpy(s.config.data(), &config, sizeof(config));
    return s;
}
static fixture_report report(Assembly& a, std::uint64_t channel = 99) {
    const auto sample = a.sample(channel);
    CHECK(sample.size() == sizeof(fixture_report));
    fixture_report r{}; std::memcpy(&r, sample.data(), sizeof(r)); return r;
}
static std::uint64_t value(const Sample& sample) {
    CHECK(sample && sample.size() >= sizeof(std::uint64_t));
    std::uint64_t v = 0; std::memcpy(&v, sample.data(), sizeof(v)); return v;
}
static void external(Assembly& a, std::uint64_t key, std::uint64_t v, std::uint64_t now, std::uint64_t expiry = 0) {
    OK(a.publish_copy(key, &v, sizeof(v), now, expiry));
}
struct Probe {
    std::mutex mutex;
    std::condition_variable cv;
    std::vector<unsigned> calls;
    std::vector<std::thread::id> threads;
    std::vector<xa_event> events;
    std::size_t steps{0};
    bool block{false}, entered{false}, released{false}, incoherent{false};
    static void callback(void* p, std::uint32_t event, const xa_frame* frame) {
        auto& s = *static_cast<Probe*>(p);
        std::unique_lock<std::mutex> lock(s.mutex);
        s.calls.push_back(event); s.threads.push_back(std::this_thread::get_id());
        if (frame) {
            ++s.steps;
            for (std::uint32_t i = 0; i < frame->event_count; ++i) s.events.push_back(frame->events[i]);
            if (frame->input_count == 2) {
                const auto& a = frame->inputs[0]; const auto& b = frame->inputs[1];
                if (a.version != b.version) s.incoherent = true;
            }
            s.entered = true;
            s.cv.notify_all();
            if (s.block) s.cv.wait(lock, [&] { return s.released; });
        }
        s.cv.notify_all();
    }
    bool wait_steps(std::size_t n) {
        std::unique_lock<std::mutex> lock(mutex);
        return cv.wait_for(lock, 5s, [&] { return steps >= n; });
    }
    bool wait_events(std::size_t n) {
        std::unique_lock<std::mutex> lock(mutex);
        return cv.wait_for(lock, 5s, [&] { return events.size() >= n; });
    }
    void release() { std::lock_guard<std::mutex> lock(mutex); released = true; cv.notify_all(); }
    fixture_config config(std::uint64_t mode = FIX_PRODUCE) {
        fixture_config c{}; c.mode = mode; c.probe = callback; c.probe_context = this; return c;
    }
};
struct Unblock { Probe& p; ~Unblock() { p.release(); } };

static void loader_validation() {
    for (auto path : {BAD_ABI_PATH, BAD_SIZE_PATH, MISSING_CALLBACK_PATH, MISSING_ENTRY_PATH}) {
        Assembly a; auto d = a.domain("test"); fixture_config c{};
        CHECK(d->add(spec(1, c, {10, 0, false, false}, 0, path)).code == XA_ABI_MISMATCH);
    }
    Assembly a; auto d = a.domain("test"); fixture_config c{};
    CHECK(d->add(spec(1, c, {}, 0, "relative.so")).code == XA_INVALID);
    CHECK(d->add(spec(1, c, {}, 0, "/does-not-exist/fixture.so")).code == XA_NOT_FOUND);
}
static void topology_validation() {
    Assembly a; OK(a.channel(1, 1)); CHECK(a.channel(1, 1).code == XA_INVALID);
    auto d = a.domain("test"); fixture_config c{}; c.output = 1;
    OK(d->add(spec(1, c)));
    CHECK(d->add(spec(2, c)).code == XA_INVALID);
    CHECK(d->add(spec(1, {})).code == XA_INVALID);
    c.output = 1000; CHECK(d->add(spec(3, c)).code == XA_INVALID);
    CHECK(d->start(0).code == XA_LIFECYCLE);
    OK(a.seal()); CHECK(a.channel(2, 1).code == XA_LIFECYCLE);
    CHECK(d->add(spec(4, {})).code == XA_LIFECYCLE);
    std::uint64_t v = 1; CHECK(a.publish_copy(1, &v, sizeof(v), 0).code == XA_INVALID);
    OK(d->start(0)); CHECK(d->start(0).code == XA_LIFECYCLE);
    OK(d->stop()); OK(d->stop()); CHECK(d->start(0).code == XA_LIFECYCLE);
}
static void periodic_and_missed() {
    Assembly a; OK(a.channel(99, 99)); auto d = a.domain("test");
    fixture_config c{}; c.report = 99; OK(d->add(spec(1, c))); OK(a.seal()); OK(d->start(0));
    OK(d->step(0)); CHECK(report(a).invocation == 1); CHECK(report(a).reasons & XA_REASON_FIRST);
    OK(d->step(9)); CHECK(report(a).invocation == 1);
    OK(d->step(10)); CHECK(report(a).invocation == 2); CHECK(report(a).elapsed == 10);
    OK(d->step(55)); CHECK(report(a).invocation == 3); CHECK(report(a).missed == 3);
    CHECK(d->stats()[0].missed_periods == 3);
    OK(d->step(59)); CHECK(report(a).invocation == 3);
    OK(d->step(60)); CHECK(report(a).invocation == 4);
    OK(d->stop());
}
static void phase_and_clock() {
    Assembly a; OK(a.channel(99, 99)); auto d = a.domain("test"); fixture_config c{}; c.report = 99;
    OK(d->add(spec(1, c, {10, 5, false, false}))); OK(a.seal()); OK(d->start(100));
    OK(d->step(104)); CHECK(!a.sample(99)); OK(d->step(105)); CHECK(report(a).invocation == 1);
    CHECK(d->step(104).code == XA_INVALID);
    CHECK(d->step(UINT64_MAX).code == XA_INVALID);
    OK(d->step(115)); CHECK(report(a).invocation == 2); OK(d->stop());
}
static void dirty_and_periodic_are_independent() {
    Assembly a; OK(a.channel(1, 1)); OK(a.channel(99, 99)); auto d = a.domain("test");
    fixture_config c{}; c.input = 1; c.report = 99;
    OK(d->add(spec(1, c, {10, 0, true, false}))); OK(a.seal()); external(a, 1, 7, 0); OK(d->start(0));
    OK(d->step(0)); CHECK(report(a).input_value == 7);
    OK(d->step(1)); CHECK(report(a).invocation == 1);
    external(a, 1, 8, 2); OK(d->step(2)); CHECK(report(a).invocation == 2);
    CHECK(report(a).reasons & XA_REASON_INPUT); CHECK(!(report(a).reasons & XA_REASON_PERIODIC));
    OK(d->step(10)); CHECK(report(a).invocation == 3); CHECK(report(a).reasons & XA_REASON_PERIODIC);
    OK(d->stop());
}
static void age_expiry_wakes_dirty() {
    Assembly a; OK(a.channel(1, 1)); OK(a.channel(99, 99)); auto d = a.domain("test");
    fixture_config c{}; c.input = 1; c.report = 99;
    auto s = spec(1, c, {0, 0, true, false}); s.inputs[0].max_age_ns = 5;
    OK(d->add(s)); OK(a.seal()); external(a, 1, 7, 0); OK(d->start(0));
    OK(d->step(0)); CHECK(report(a).input_flags & XA_INPUT_VALID);
    OK(d->step(5)); CHECK(report(a).invocation == 1);
    OK(d->step(6)); CHECK(report(a).invocation == 2);
    CHECK(!(report(a).input_flags & XA_INPUT_VALID)); CHECK(report(a).reasons & XA_REASON_VALIDITY);
    OK(d->step(7)); CHECK(report(a).invocation == 2); OK(d->stop());
}
static void publisher_expiry_and_future_input() {
    Assembly a; OK(a.channel(1, 1)); OK(a.channel(99, 99)); auto d = a.domain("test");
    fixture_config c{}; c.input = 1; c.report = 99;
    OK(d->add(spec(1, c, {0, 0, true, false}))); OK(a.seal()); external(a, 1, 7, 10, 15); OK(d->start(0));
    OK(d->step(0)); CHECK(!(report(a).input_flags & XA_INPUT_VALID));
    OK(d->step(10)); CHECK(report(a).input_flags & XA_INPUT_VALID);
    OK(d->step(15)); CHECK(!(report(a).input_flags & XA_INPUT_VALID)); OK(d->stop());
}
static void same_stage_snapshot() {
    Assembly a; OK(a.channel(1, 1)); OK(a.channel(99, 99)); auto d = a.domain("test");
    fixture_config p{}; p.output = 1; p.value = 100;
    fixture_config q{}; q.input = 1; q.report = 99;
    OK(d->add(spec(1, p))); OK(d->add(spec(2, q))); OK(a.seal()); OK(d->start(0));
    OK(d->step(0)); CHECK(!(report(a).input_flags & XA_INPUT_PRESENT)); CHECK(value(a.sample(1)) == 101);
    OK(d->step(10)); CHECK(report(a).input_value == 101); CHECK(value(a.sample(1)) == 102); OK(d->stop());
}
static void downstream_stage_current_sample() {
    Assembly a; OK(a.channel(1, 1)); OK(a.channel(99, 99)); auto d = a.domain("test");
    fixture_config p{}; p.output = 1; p.value = 100;
    fixture_config q{}; q.input = 1; q.report = 99;
    OK(d->add(spec(2, q, {10, 0, false, false}, 1))); OK(d->add(spec(1, p)));
    OK(a.seal()); OK(d->start(0)); OK(d->step(0)); CHECK(report(a).input_value == 101);
    CHECK(report(a).input_version == a.sample(1).version); OK(d->stop());
}
static void independent_fsm_events_are_deferred() {
    Assembly a; OK(a.channel(99, 99)); auto d = a.domain("test");
    fixture_config p{}; p.target = 2; p.emit_mode = FIX_EMIT_FIRST;
    fixture_config q{}; q.mode = FIX_FSM; q.report = 99;
    OK(d->add(spec(1, p))); OK(d->add(spec(2, q, {0, 0, false, true}, 1)));
    OK(a.seal()); OK(d->start(0)); OK(d->step(0)); CHECK(!a.sample(99));
    OK(d->step(0)); auto r = report(a); CHECK(r.state == 1); CHECK(r.event_source == 1);
    CHECK(r.correlation == 123 && r.generation == 7 && r.events == 1);
    xa_event stop{}; stop.kind = 2; stop.source = 999; OK(a.post(2, stop));
    OK(d->step(1)); CHECK(report(a).state == 0); CHECK(report(a).event_source == 0); OK(d->stop());
}
static void bounded_fifo_and_budget() {
    Limits l; l.mailbox_events = 3; l.events_per_step = 2;
    Assembly a(l); OK(a.channel(99, 99)); auto d = a.domain("test"); fixture_config c{}; c.report = 99;
    OK(d->add(spec(1, c, {0, 0, false, true}))); OK(a.seal()); OK(d->start(0));
    for (std::uint64_t i = 1; i <= 3; ++i) { xa_event e{}; e.kind = 1; e.value[0] = i; OK(a.post(1, e)); }
    xa_event e{}; e.kind = 1; CHECK(a.post(1, e).code == XA_LIMIT);
    OK(d->step(0)); CHECK(report(a).events == 2); CHECK(report(a).first_event == 1 && report(a).last_event == 2);
    OK(d->step(0)); CHECK(report(a).events == 1 && report(a).first_event == 3);
    OK(d->step(0)); CHECK(report(a).invocation == 2); OK(d->stop());
    CHECK(a.post(1, e).code == XA_LIFECYCLE); CHECK(a.post(1000, e).code == XA_NOT_FOUND);
}
static void queued_events_wait_for_periodic_consumer() {
    Assembly a; OK(a.channel(99, 99)); auto d = a.domain("test"); fixture_config c{}; c.report = 99;
    OK(d->add(spec(1, c, {10, 5, false, false}))); OK(a.seal()); OK(d->start(0));
    xa_event e{}; e.kind = 1; OK(a.post(1, e)); OK(d->step(0)); CHECK(!a.sample(99));
    OK(d->step(5)); CHECK(report(a).events == 1); OK(d->stop());
}
static void callback_failure_rolls_back() {
    Assembly a; OK(a.channel(1, 1)); OK(a.channel(99, 99)); auto d = a.domain("test");
    fixture_config p{}; p.mode = FIX_FAIL; p.output = 1; p.target = 2; p.emit_mode = FIX_EMIT_EVERY;
    fixture_config q{}; q.report = 99;
    OK(d->add(spec(1, p))); OK(d->add(spec(2, q, {0, 0, false, true})));
    OK(a.seal()); OK(d->start(0)); CHECK(d->step(0).code == XA_PLUGIN_ERROR);
    CHECK(!a.sample(1)); OK(d->step(1)); CHECK(!a.sample(99));
    CHECK(d->stats()[0].lifecycle == Lifecycle::faulted); CHECK(d->stats()[0].failures == 1);
    OK(d->stop());
}
static void mailbox_rejection_rolls_back_all_outputs() {
    Limits l; l.mailbox_events = 1;
    Assembly a(l); OK(a.channel(1, 1)); OK(a.channel(2, 1)); OK(a.channel(99, 99)); auto d = a.domain("test");
    fixture_config p{}; p.output = 1; p.output2 = 2; p.target = 2; p.emit_mode = FIX_EMIT_EVERY;
    fixture_config q{}; q.report = 99;
    OK(d->add(spec(1, p))); OK(d->add(spec(2, q, {0, 0, false, true})));
    OK(a.seal()); xa_event e{}; e.kind = 1; e.value[0] = 42; OK(a.post(2, e)); OK(d->start(0));
    CHECK(d->step(0).code == XA_LIMIT); CHECK(!a.sample(1) && !a.sample(2));
    CHECK(report(a).first_event == 42 && report(a).event_source == 0);
    CHECK(d->stats()[0].rejected_transactions == 1); OK(d->stop());
}
static void schema_token_and_ignored_error_rejection() {
    for (auto mode : {FIX_BAD_SCHEMA, FIX_BAD_TOKEN, FIX_IGNORE_ERROR, FIX_RETAIN_MUTABLE}) {
        Assembly a; OK(a.channel(1, 1)); auto d = a.domain("test"); fixture_config c{}; c.mode = mode; c.output = 1;
        OK(d->add(spec(1, c))); OK(a.seal()); OK(d->start(0)); CHECK(d->step(0).code == XA_INVALID);
        CHECK(!a.sample(1)); CHECK(a.live_buffer_bytes() == 0); OK(d->stop());
    }
}
static void large_buffer_is_not_copied_between_components() {
    Sample saved;
    const void* address = nullptr;
    {
        Assembly a; OK(a.channel(1, 1)); OK(a.channel(2, 1)); auto d = a.domain("test");
        fixture_config c{}; c.mode = FIX_FORWARD; c.input = 1; c.output = 2;
        OK(d->add(spec(1, c, {0, 0, true, false}))); OK(a.seal());
        std::vector<std::uint64_t> data(512 * 1024, 42);
        OK(a.publish_copy(1, data.data(), data.size() * sizeof(data[0]), 0));
        address = a.sample(1).data();
        OK(d->start(0)); OK(d->step(0)); saved = a.sample(2);
        CHECK(saved.data() == address); CHECK(a.live_buffer_bytes() == data.size() * sizeof(data[0]));
        OK(d->stop());
    }
    CHECK(saved.data() == address); CHECK(value(saved) == 42);
}
static void retained_buffer_lifetime() {
    Assembly a; OK(a.channel(1, 1)); OK(a.channel(99, 99)); auto d = a.domain("test");
    fixture_config c{}; c.mode = FIX_RETAIN; c.input = 1; c.report = 99;
    OK(d->add(spec(1, c))); OK(a.seal()); external(a, 1, 11, 0); OK(d->start(0)); OK(d->step(0));
    CHECK(report(a).retained_value == 11);
    external(a, 1, 22, 10); OK(d->step(10));
    CHECK(report(a).input_value == 22 && report(a).retained_value == 11);
    CHECK(a.live_buffer_bytes() == sizeof(std::uint64_t) + sizeof(fixture_report));
    OK(d->step(20)); CHECK(report(a).retained_value == 22); OK(d->stop());
}
static void buffer_budget_failure_is_explicit() {
    Limits l; l.buffer_bytes = 8;
    Assembly a(l); OK(a.channel(1, 1)); auto d = a.domain("test"); fixture_config c{}; c.output = 1; c.allocation_size = 16;
    OK(d->add(spec(1, c))); OK(a.seal()); OK(d->start(0)); CHECK(d->step(0).code == XA_LIMIT);
    CHECK(!a.sample(1) && a.live_buffer_bytes() == 0); OK(d->stop());
}
static void startup_failure_cleans_up() {
    for (auto mode : {FIX_CREATE_FAIL, FIX_START_FAIL, FIX_NULL_INSTANCE}) {
        Probe first, second;
        Assembly a; auto d = a.domain("test");
        OK(d->add(spec(1, first.config()))); OK(d->add(spec(2, second.config(mode))));
        OK(a.seal()); CHECK(d->start(0).code == XA_PLUGIN_ERROR);
        CHECK((first.calls == std::vector<unsigned>{1, 2, 4, 5}));
        if (mode == FIX_START_FAIL) CHECK((second.calls == std::vector<unsigned>{1, 2, 4, 5}));
        if (mode == FIX_CREATE_FAIL) CHECK((second.calls == std::vector<unsigned>{1, 5}));
        if (mode == FIX_NULL_INSTANCE) CHECK((second.calls == std::vector<unsigned>{1}));
        OK(d->stop()); CHECK(d->step(0).code == XA_LIFECYCLE);
    }
}
static void stop_error_still_destroys() {
    Probe p; Assembly a; auto d = a.domain("test");
    OK(d->add(spec(1, p.config(FIX_STOP_FAIL)))); OK(a.seal()); OK(d->start(0));
    CHECK(d->stop().code == XA_PLUGIN_ERROR); CHECK((p.calls == std::vector<unsigned>{1, 2, 4, 5}));
    OK(d->stop());
}
static void owner_thread_is_enforced() {
    Assembly a; auto d = a.domain("test"); OK(d->add(spec(1, {}))); OK(a.seal()); OK(d->start(0));
    xa_status step_status = XA_OK, stop_status = XA_OK;
    std::thread wrong([&] { step_status = d->step(0).code; stop_status = d->stop().code; }); wrong.join();
    CHECK(step_status == XA_WRONG_THREAD && stop_status == XA_WRONG_THREAD); OK(d->stop());
}
static void exceptions_and_foreign_services_are_contained() {
    for (auto mode : {FIX_THROW, FIX_FOREIGN_THREAD}) {
        Assembly a; OK(a.channel(99, 99)); auto d = a.domain("test"); fixture_config c{}; c.mode = mode; c.report = 99;
        OK(d->add(spec(1, c, {10, 0, false, false}, 0, EXCEPTION_PATH))); OK(a.seal()); OK(d->start(0));
        if (mode == FIX_THROW) { CHECK(d->step(0).code == XA_PLUGIN_ERROR); CHECK(!a.sample(99)); }
        else { OK(d->step(0)); CHECK(report(a).foreign_status == XA_WRONG_THREAD); }
        OK(d->stop());
    }
}
static void independent_domains_and_lifecycle_affinity() {
    Probe slow, fast; slow.block = true;
    Assembly a; auto slow_domain = a.domain("slow"); auto fast_domain = a.domain("fast");
    OK(slow_domain->add(spec(1, slow.config(), {100000, 0, false, false})));
    OK(fast_domain->add(spec(2, fast.config(), {100000, 0, false, false}))); OK(a.seal());
    DomainThread slow_worker(slow_domain, 100000), fast_worker(fast_domain, 100000);
    Unblock unblock{slow};
    OK(slow_worker.start()); CHECK(slow.wait_steps(1));
    CHECK(slow_domain->step(0).code == XA_BUSY);
    OK(fast_worker.start()); CHECK(fast.wait_steps(20));
    fast_worker.stop(); slow.release(); slow_worker.stop();
    OK(slow_worker.result()); OK(fast_worker.result());
    CHECK(slow.threads.front() != fast.threads.front());
    CHECK(std::all_of(slow.threads.begin(), slow.threads.end(), [&](auto id) { return id == slow.threads.front(); }));
    CHECK(std::all_of(fast.threads.begin(), fast.threads.end(), [&](auto id) { return id == fast.threads.front(); }));
    CHECK(slow.calls.back() == 5 && fast.calls.back() == 5);
    CHECK(slow_worker.start().code == XA_LIFECYCLE);
}
static void threaded_mailbox_concurrency() {
    Limits l; l.mailbox_events = 3000; l.events_per_step = 17;
    Probe p; Assembly a(l); auto d = a.domain("test");
    OK(d->add(spec(1, p.config(), {0, 0, false, true}))); OK(a.seal());
    DomainThread worker(d, 100000); OK(worker.start());
    std::atomic<unsigned> failures{0}; std::vector<std::thread> senders;
    for (std::uint64_t sender = 0; sender < 4; ++sender) {
        senders.emplace_back([&, sender] {
            for (std::uint64_t n = 1; n <= 500; ++n) {
                xa_event e{}; e.kind = 1; e.value[0] = (sender << 32) | n;
                if (!a.post(1, e)) ++failures;
            }
        });
    }
    for (auto& sender : senders) sender.join();
    CHECK(p.wait_events(2000)); worker.stop(); OK(worker.result()); CHECK(failures.load() == 0);
    std::uint64_t last[4]{};
    CHECK(p.events.size() == 2000);
    for (const auto& e : p.events) {
        const auto sender = e.value[0] >> 32, sequence = e.value[0] & UINT32_MAX;
        CHECK(sender < 4); CHECK(sequence == ++last[sender]); CHECK(e.source == 0);
    }
}
static void cross_domain_multi_output_snapshot_is_coherent() {
    Probe observer;
    Assembly a; OK(a.channel(1, 1)); OK(a.channel(2, 1));
    auto p = a.domain("producer"), q = a.domain("observer");
    fixture_config c{}; c.output = 1; c.output2 = 2;
    OK(p->add(spec(1, c, {100000, 0, false, false})));
    auto s = spec(2, observer.config(), {100000, 0, false, false}); s.inputs = {{1, 0}, {2, 0}};
    OK(q->add(s)); OK(a.seal());
    DomainThread pw(p, 100000), qw(q, 100000); OK(pw.start()); OK(qw.start());
    CHECK(observer.wait_steps(100)); qw.stop(); pw.stop(); OK(qw.result()); OK(pw.result());
    CHECK(!observer.incoherent); CHECK(a.sample(1).version == a.sample(2).version);
    CHECK(a.sample(1).data() == a.sample(2).data());
}
static void multiple_instances_do_not_share_state() {
    Assembly a; OK(a.channel(1, 1)); OK(a.channel(2, 1)); auto d = a.domain("test");
    fixture_config p{}; p.output = 1; p.value = 10; fixture_config q{}; q.output = 2; q.value = 100;
    OK(d->add(spec(1, p))); OK(d->add(spec(2, q))); OK(a.seal()); OK(d->start(0)); OK(d->step(0));
    CHECK(value(a.sample(1)) == 11 && value(a.sample(2)) == 101); OK(d->step(10));
    CHECK(value(a.sample(1)) == 12 && value(a.sample(2)) == 102); OK(d->stop());
}

static void nested_domain_execution_is_rejected() {
    struct Nested {
        Domain* own;
        Domain* other;
        unsigned calls{0};
        bool correct{true};
        static void callback(void* opaque, std::uint32_t, const xa_frame*) {
            auto& n = *static_cast<Nested*>(opaque);
            ++n.calls;
            for (auto* d : {n.own, n.other}) {
                n.correct = n.correct && d->start(0).code == XA_BUSY;
                n.correct = n.correct && d->step(0).code == XA_BUSY;
                n.correct = n.correct && d->stop().code == XA_BUSY;
                n.correct = n.correct && d->add(ComponentSpec{}).code == XA_BUSY;
            }
            n.correct = n.correct && n.own->stats().size() == 1;
            bool threw = false;
            try { (void)n.other->stats(); }
            catch (const std::logic_error&) { threw = true; }
            n.correct = n.correct && threw;
        }
    };
    Assembly a;
    auto own = a.domain("owner"), other = a.domain("other");
    Nested n{own.get(), other.get()};
    fixture_config c{}; c.probe = Nested::callback; c.probe_context = &n;
    OK(own->add(spec(1, c))); OK(other->add(spec(2, {}))); OK(a.seal());
    OK(other->start(0)); OK(own->start(0)); OK(own->step(0)); OK(own->stop()); OK(other->stop());
    CHECK(n.calls == 5); CHECK(n.correct);
}


static void driver_callback_uses_nonjoining_stop_request() {
    struct Nested {
        DomainThread* driver{nullptr};
        unsigned calls{0};
        std::promise<void> requested;
        bool correct{true};
        static void callback(void* opaque, std::uint32_t event, const xa_frame*) {
            auto& n = *static_cast<Nested*>(opaque);
            ++n.calls;
            n.correct = n.correct && n.driver->start().code == XA_BUSY;
            n.correct = n.correct && n.driver->stop().code == XA_BUSY;
            if (event == 3) { n.driver->request_stop(); n.requested.set_value(); }
        }
    } n;
    Assembly a; auto d = a.domain("test");
    fixture_config c{}; c.probe = Nested::callback; c.probe_context = &n;
    OK(d->add(spec(1, c))); OK(a.seal());
    DomainThread worker(d, 100000); n.driver = &worker;
    auto requested = n.requested.get_future();
    OK(worker.start()); CHECK(requested.wait_for(5s) == std::future_status::ready);
    OK(worker.stop()); OK(worker.result());
    CHECK(n.calls == 5); CHECK(n.correct);
}


static void driver_preserves_busy_callback_error_during_cleanup() {
    Probe p; Assembly a; auto d = a.domain("test");
    OK(d->add(spec(1, p.config(FIX_BUSY)))); OK(a.seal());
    {
        DomainThread worker(d, 100000); OK(worker.start()); CHECK(p.wait_steps(1));
        CHECK(worker.stop().code == XA_BUSY); CHECK(worker.result().code == XA_BUSY);
    }
    CHECK(p.calls.size() == 5 && p.calls.back() == 5);
}

static void invalid_schedule_and_limits() {
    Assembly a; auto d = a.domain("test");
    CHECK(d->add(spec(1, {}, {0, 0, false, false})).code == XA_INVALID);
    CHECK(d->add(spec(1, {}, {0, 1, false, true})).code == XA_INVALID);
    CHECK(d->add(spec(1, {}, {UINT64_MAX, 0, false, false})).code == XA_INVALID);
    CHECK(a.seal().code == XA_INVALID);
    bool threw = false; try { Limits l; l.buffers = 0; Assembly bad(l); } catch (const std::invalid_argument&) { threw = true; }
    CHECK(threw);
    threw = false;
    try { Limits l; l.events_per_step = static_cast<std::size_t>(UINT32_MAX) + 1; Assembly bad(l); }
    catch (const std::invalid_argument&) { threw = true; }
    CHECK(threw);
}
int main() {
    const std::vector<std::pair<const char*, std::function<void()>>> tests{
#define TEST(name) {#name, name}
        TEST(loader_validation), TEST(topology_validation), TEST(periodic_and_missed), TEST(phase_and_clock),
        TEST(dirty_and_periodic_are_independent), TEST(age_expiry_wakes_dirty), TEST(publisher_expiry_and_future_input),
        TEST(same_stage_snapshot), TEST(downstream_stage_current_sample), TEST(independent_fsm_events_are_deferred),
        TEST(bounded_fifo_and_budget), TEST(queued_events_wait_for_periodic_consumer), TEST(callback_failure_rolls_back),
        TEST(mailbox_rejection_rolls_back_all_outputs), TEST(schema_token_and_ignored_error_rejection),
        TEST(large_buffer_is_not_copied_between_components), TEST(retained_buffer_lifetime),
        TEST(buffer_budget_failure_is_explicit), TEST(startup_failure_cleans_up), TEST(stop_error_still_destroys),
        TEST(owner_thread_is_enforced), TEST(exceptions_and_foreign_services_are_contained),
        TEST(independent_domains_and_lifecycle_affinity), TEST(threaded_mailbox_concurrency),
        TEST(cross_domain_multi_output_snapshot_is_coherent), TEST(multiple_instances_do_not_share_state),
        TEST(nested_domain_execution_is_rejected), TEST(driver_callback_uses_nonjoining_stop_request),
        TEST(driver_preserves_busy_callback_error_during_cleanup),
        TEST(invalid_schedule_and_limits)
#undef TEST
    };
    unsigned failed = 0;
    for (const auto& [name, test] : tests) {
        try { test(); std::cout << "PASS " << name << '\n'; }
        catch (const std::exception& e) { ++failed; std::cerr << "FAIL " << name << ": " << e.what() << '\n'; }
    }
    std::cout << (tests.size() - failed) << '/' << tests.size() << " contract cases passed\n";
    return failed ? 1 : 0;
}
