#include "xgc2/assembly/assembly.hpp"
#include <cstring>
#include <filesystem>
#include <iostream>
#include <stdexcept>
using namespace xgc2::assembly;
static void check(Status s) { if (!s) throw std::runtime_error(s.message); }
int main(int argc, char** argv) {
    if (argc != 2) { std::cerr << "Usage: xa_demo /absolute/path/libxa_scalar.so\n"; return 2; }
    try {
        Assembly assembly;
        check(assembly.channel(1, 1));
        auto domain = assembly.domain("main");
        ComponentSpec spec;
        spec.id = 1;
        spec.library = std::filesystem::absolute(argv[1]).string();
        spec.outputs = {1};
        spec.schedule = {1000000, 0, false, false};
        const std::uint64_t channel = 1;
        spec.config.resize(sizeof(channel));
        std::memcpy(spec.config.data(), &channel, sizeof(channel));
        check(domain->add(std::move(spec)));
        check(assembly.seal()); check(domain->start(0));
        for (std::uint64_t tick = 0; tick != 5; ++tick) {
            check(domain->step(tick * 1000000));
            const auto sample = assembly.sample(1);
            std::uint64_t value = 0;
            if (sample.size() != sizeof(value)) throw std::runtime_error("invalid output");
            std::memcpy(&value, sample.data(), sizeof(value));
            std::cout << "version=" << sample.version << " value=" << value << '\n';
        }
        check(domain->stop());
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
