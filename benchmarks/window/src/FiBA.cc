#include "window/include/FiBA.h"
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
