#include "uwheel_bench/include/FiBA.h"
#include <iostream>

std::unique_ptr<FiBA_SUM> create_fiba_with_sum() {
  return std::make_unique<FiBA_SUM>(Sum<uint64_t>());
}

std::unique_ptr<FiBA_SUM_4> create_fiba_4_with_sum() {
  return std::make_unique<FiBA_SUM_4>(Sum<uint64_t>());
}

std::unique_ptr<FiBA_SUM_8> create_fiba_8_with_sum() {
  return std::make_unique<FiBA_SUM_8>(Sum<uint64_t>());
}

std::unique_ptr<Bclassic_2> create_bclassic_2_with_sum() {
  return std::make_unique<Bclassic_2>(Sum<uint64_t>());
}

std::unique_ptr<Bclassic_4> create_bclassic_4_with_sum() {
  return std::make_unique<Bclassic_4>(Sum<uint64_t>());
}

std::unique_ptr<Bclassic_8> create_bclassic_8_with_sum() {
  return std::make_unique<Bclassic_8>(Sum<uint64_t>());
}
