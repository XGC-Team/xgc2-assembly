#include "xgc2/assembly/assembly.hpp"
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <thread>

namespace xgc2::assembly {
namespace {
thread_local void* active_driver = nullptr;
struct DriverScope {
    explicit DriverScope(void* driver) { active_driver = driver; }
    ~DriverScope() { active_driver = nullptr; }
};
std::uint64_t now_ns() {
    return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count());
}
}
struct DomainThread::Impl {
    std::shared_ptr<Domain> domain;
    std::chrono::nanoseconds period;
    std::thread worker;
    std::atomic<bool> stopping{false};
    mutable std::mutex result_mutex;
    std::mutex driver_mutex, wait_mutex;
    std::condition_variable cv;
    bool used{false};
    Status outcome;
    Impl(std::shared_ptr<Domain> d, std::uint64_t ns) : domain(std::move(d)), period(ns) {}
    void set_result(Status s) {
        if (!s) { std::lock_guard<std::mutex> lock(result_mutex); if (outcome) outcome = std::move(s); }
    }
};
DomainThread::DomainThread(std::shared_ptr<Domain> domain, std::uint64_t period) {
    if (!domain || !period || period > 86400ull * 1000000000ull) throw std::invalid_argument("invalid domain or base period");
    impl_ = std::make_unique<Impl>(std::move(domain), period);
}
DomainThread::~DomainThread() {
    if (active_driver) std::terminate();
    (void)stop();
}
Status DomainThread::start() {
    if (active_driver) return {XA_BUSY, "cannot start a domain driver from a driver callback"};
    auto& d = *impl_;
    std::lock_guard<std::mutex> guard(d.driver_mutex);
    if (d.used) return {XA_LIFECYCLE, "driver is one-shot"};
    d.used = true;
    std::promise<Status> ready;
    auto future = ready.get_future();
    try {
        d.worker = std::thread([&d, signal = std::move(ready)]() mutable {
            DriverScope scope(&d);
            bool started = false;
            bool signalled = false;
            try {
                auto status = d.domain->start(now_ns());
                started = static_cast<bool>(status);
                signal.set_value(status); signalled = true;
                d.set_result(status);
                auto next = std::chrono::steady_clock::now();
                while (started && !d.stopping.load(std::memory_order_acquire)) {
                    status = d.domain->step(now_ns());
                    if (!status) { d.set_result(status); break; }
                    const auto now = std::chrono::steady_clock::now();
                    if (next <= now) next += d.period * ((now - next) / d.period + 1);
                    std::unique_lock<std::mutex> lock(d.wait_mutex);
                    d.cv.wait_until(lock, next, [&d] { return d.stopping.load(std::memory_order_acquire); });
                }
            } catch (...) {
                Status s{XA_PLUGIN_ERROR, "domain driver failed"};
                d.set_result(s);
                if (!signalled) signal.set_value(s);
            }
            if (started) d.set_result(d.domain->stop());
        });
    } catch (...) {
        Status status{XA_LIMIT, "cannot create domain thread"};
        d.set_result(status);
        return status;
    }
    return future.get();
}
void DomainThread::request_stop() noexcept {
    auto& d = *impl_;
    d.stopping.store(true, std::memory_order_release);
    // Synchronize with wait predicate evaluation to avoid a lost stop wakeup.
    { std::lock_guard<std::mutex> lock(d.wait_mutex); }
    d.cv.notify_all();
}
Status DomainThread::stop() {
    if (active_driver) return {XA_BUSY, "use request_stop from a driver callback"};
    auto& d = *impl_;
    std::lock_guard<std::mutex> guard(d.driver_mutex);
    request_stop();
    if (d.worker.joinable()) d.worker.join();
    return result();
}
Status DomainThread::result() const {
    std::lock_guard<std::mutex> lock(impl_->result_mutex);
    return impl_->outcome;
}
} // namespace xgc2::assembly
