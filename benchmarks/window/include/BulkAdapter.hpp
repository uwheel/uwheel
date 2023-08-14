#pragma once
#include<vector>
#include<utility>
#include<cstdio>

template <class Base, typename timeT, typename inT>
class BulkAdapter : public Base {
public:
  using Base::Base;

  void bulkInsert(std::vector<std::pair<timeT, inT>> entries) {
    bulkInsert(entries.begin(), entries.end());
  }

  template <class Iterator>
  void bulkInsert(Iterator begin, Iterator end) {
      for (auto it=begin;it!=end;it++) {
          Base::insert(it->first, it->second);
      }
  }

  void bulkEvict(timeT const& time) {
#ifdef _COLLECT_STATS
      int count=0;
#endif
      while (Base::size() > 0 && Base::oldest() <= time) {
          Base::evict();
#ifdef _COLLECT_STATS
          count++;
#endif
      }
#ifdef _COLLECT_STATS
      printf("== bulkEvict: count=%d\n", count);
#endif
  }
};