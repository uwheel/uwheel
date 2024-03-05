#ifndef __AGGREGATATION_FUNCTIONS_
#define __AGGREGATATION_FUNCTIONS_

#include <iostream>
#include <cstdint>
#include <cmath>
#include <bitset>
#include <functional>
#include <vector>
#include <list>
#include <limits>

template <class _In, class _Partial=_In, class _Out=_In>
class Sum {
public:
    typedef _In In;
    typedef _Partial Partial;
    typedef _Out Out;

    Out lower(const Partial& c) const {
        return static_cast<Out>(c);
    }

    Partial lift(const In& v) const {
        return static_cast<Partial>(v);
    }

    Partial combine(const Partial& a, const Partial& b) const {
        return a + b;
    }

    Partial inverse_combine(const Partial& a, const Partial& b) const {
        return a - b;
    }

    void recalc_combine(Partial& accum, const In& b) const {
        accum += b;
    }

    std::string name() const { return "sum"; }

    static const Partial identity;
};

template <typename I, typename P, typename O>
const typename Sum<I,P,O>::Partial Sum<I,P,O>::identity = P();

//template <>
//const typename Sum<int>::Partial Sum<int>::identity = 0;

template <class _In, class _Partial=_In, class _Out=_In>
class Max {
public:
    typedef _In In;
    typedef _Partial Partial;
    typedef _Out Out;

    Out lower(const Partial& c) const {
        return static_cast<Out>(c);
    }

    Partial lift(const In& v) const {
        return static_cast<Partial>(v);
    }

    Partial combine(const Partial& a, const Partial& b) const {
        if (a > b) {
            return a;
        }
        return b;
    }

    void recalc_combine(Partial& accum, const In& b) const {
        if (b > accum) {
            accum = b;
        }
    }

  std::string name() const { return "max"; }

  static const Partial identity;
};

template <typename I, typename P, typename O>
const typename Max<I,P,O>::Partial Max<I,P,O>::identity = std::numeric_limits<P>::min();

//template <>
//const typename Max<int>::Partial Max<int>::identity = std::numeric_limits<int>::min();

template <class _In>
class Mean {
public:
    typedef _In In;
    struct Partial {
        In sum;
        uint64_t n;
        bool operator==(Partial const& p) const {
            return sum == p.sum && n == p.n;
        }
        bool operator!=(Partial const& p) const { return !(*this == p); }
        friend inline std::ostream& operator<<(std::ostream& os, Partial const& p) {
            return os << "{" << p.sum << ", " << p.n << "}";
        }
    };
    typedef In Out;

    Out lower(const Partial& c) const {
        return static_cast<Out>(c.sum / c.n);
    }

    Partial lift(const In& v) const {
        Partial part;
        part.sum = v;
        part.n = 1;
        return part;
    }

    Partial combine(const Partial& a, const Partial& b) const {
        Partial part;
        part.n = a.n + b.n;
        part.sum = a.sum + b.sum;
        return part;
    }

    Partial inverse_combine(const Partial& a, const Partial& b) const {
        Partial part;
        part.n = a.n - b.n;
        part.sum = a.sum - b.sum;
        return part;
    }

    void recalc_combine(Partial& accum, const In& b) const {
        accum.sum += b;
        ++accum.n;
    }

  std::string name() const { return "mean"; }

  static const Partial identity;
};

template <class In>
const typename Mean<In>::Partial Mean<In>::identity = {In(), 0};

template <class _In, class _Out=_In>
class GeometricMean {
public:
    typedef _In In;
    struct Partial {
        double product;
        uint32_t n;
        bool operator==(Partial const& p) const {
            //for invariant checking, tolerate epsilon difference
            if (p.n != n)
                return false;
            if (product == 0.0 || std::isnan(product) || std::isinf(product))
                return p.product == product;
            double quotient = p.product / product;
            double epsilon = 0.00000001;
            return 1.0 - epsilon < quotient && quotient < 1.0 + epsilon;
        }
        bool operator!=(Partial const& p) const {
            //hack, not symmetric: for short-circuiting, require exact match!
            return p.n != n || p.product != product;
        }
        friend inline std::ostream& operator<<(std::ostream& os, Partial const& p) {
            return os << "{" << p.product << ", " << p.n << "}";
        }
    };
    typedef _Out Out;

    Out lower(const Partial& c) const {
        float t = static_cast<float>(c.product/(float) c.n); // the exponent
        float geo_mean = expf(t);
        return static_cast<Out>(geo_mean);
    }

    Partial lift(const In& v) const {
        Partial part;
        part.product = std::log(static_cast<double>(v));
        part.n = 1;
        return part;
    }

    Partial combine(const Partial& a, const Partial& b) const {
        Partial part;
        part.product = a.product + b.product;
        part.n = a.n + b.n;
        return part;
    }

    Partial inverse_combine(const Partial& a, const Partial& b) const {
        Partial part;
        part.product = a.product - b.product;
        part.n = a.n - b.n;
        return part;
    }

    void recalc_combine(Partial& accum, const In& b) const {
        accum.product += std::log(static_cast<double>(b));
        ++accum.n;
    }

  std::string name() const { return "geometric-mean"; }

    static const Partial identity;
};

template <class I, class O>
const typename GeometricMean<I,O>::Partial GeometricMean<I,O>::identity = {1.0, 0};

template <class _In>
class SampleStdDev {
public:
    typedef _In In;
    struct Partial {
        In sum;
        In sq;
        uint64_t n;
        bool operator==(Partial const& p) const {
            return sum == p.sum && sq == p.sq && n == p.n;
        }
        bool operator!=(Partial const& p) const { return !(*this == p); }
        friend inline std::ostream& operator<<(std::ostream& os, Partial const& p) {
            return os << "{" << p.sum << ", " << p.sq << ", " << p.n << "}";
        }
    };
    typedef In Out;

    Out lower(const Partial& c) const {
        return static_cast<Out>(std::sqrt((1.0 / (c.n - 1)) * (c.sq - ((c.sum * c.sum) / static_cast<double>(c.n)))));
    }

    Partial lift(const In& v) const {
        Partial part;
        part.sum = v;
        part.sq = v * v;
        part.n = 1;
        return part;
    }

    Partial combine(const Partial& a, const Partial& b) const {
        Partial part;
        part.sum = a.sum + b.sum;
        part.sq = a.sq + b.sq;
        part.n = a.n + b.n;
        return part;
    }

    Partial inverse_combine(const Partial& a, const Partial& b) const {
        Partial part;
        part.sum = a.sum - b.sum;
        part.sq = a.sq - b.sq;
        part.n = a.n - b.n;
        return part;
    }

    void recalc_combine(Partial& accum, const In& b) const {
        accum.sum += b;
        accum.sq += b * b;
        ++accum.n;
    }

  std::string name() const { return "sample-std-dev"; }

    static const Partial identity;
};

template <class In>
const typename SampleStdDev<In>::Partial SampleStdDev<In>::identity = {In(), In(), 0};

template <class _In, class Comparable, class InLift>
class ArgMax {
public:
    typedef _In In;
    struct Partial {
        In arg;
        Comparable max;
        bool operator==(Partial const& p) const {
            return arg == p.arg && max == p.max;
        }
        bool operator!=(Partial const& p) const { return !(*this == p); }
        friend inline std::ostream& operator<<(std::ostream& os, Partial const& p) {
            return os << "{" << p.arg << ", " << p.max << "}";
        }
    };
    typedef In Out;

    Out lower(const Partial& c) const {
        return c.arg;
    }

    Partial lift(const In& v) const {
        Partial part;
        part.arg = v;
        part.max = _lifter(v);
        return part;
    }

    Partial combine(const Partial& a, const Partial& b) const {
        if (a.max >= b.max) {
            return a;
        }
        return b;
    }

    void recalc_combine(Partial& accum, const In& b) const {
        if (_lifter(b) >= accum.max) {
            accum.max = _lifter(b);
            accum.arg = b;
        }
    }

  std::string name() const { return "arg-max"; }

    static const Partial identity;

private:
    InLift _lifter;
};

template <class I, class C, class IL>
const typename ArgMax<I, C, IL>::Partial ArgMax<I, C, IL>::identity = {I(), C()};

// depends on std::hash providing a hash function; otherwise, you should supply
// one.
template <typename _In, typename _Out=_In, int N=4096, int K=4>
class BloomFilter {
public:
  typedef _In In;
  typedef std::bitset<N> Partial;
  typedef _Out Out;

  BloomFilter() {
    mkSalt(K);
  }

  Partial lift(const In& v) const {
    std::bitset<N> b;

    std::size_t hv = mix(101, static_cast<std::size_t>(v));
    for (int i=0;i<_salt.size();i++) {
      std::size_t h2 = mix(hv, _salt[i]);
      b.set(h2%N);
    }
    return b;
  }

  Partial combine(const Partial &a, const Partial &b) const {
    return a | b;
  }

  Out lower(const Partial &bs) const {
    size_t count = bs.test(0);
    return count;
  }

  void recalc_combine(Partial& accum, const In& b) const {
    accum |= b;
  }

  std::string name() const { return "bloom-filter"; }

  static const Partial identity;

private:
  inline size_t mix(std::size_t a, std::size_t b) const {
      unsigned long i1 = b >> 32;
      unsigned long i2 = b & ((1L << 32)-1);
      std::size_t h2 = a ^ (a <<  7) ^  i1 * (a >> 3) ^ (~((a << 11) + (i2 ^ (a >> 5))));
      return h2;
  }

  void mkSalt(int numSalts) {
    srand(1001); // fix the starting point
    for (int i=0;i<numSalts;i++) {
      _salt.push_back(rand());
    }
  }

  std::vector<long> _salt;
};

template <class I, class O, int N, int K>
const typename BloomFilter<I, O, N, K>::Partial BloomFilter<I, O, N, K>::identity = std::bitset<N>();

template <class _In, class _List=std::list<_In>>
class Collect {
public:
    typedef _In In;
    struct Partial {
        _List ls;
        bool operator==(Partial const& p) const {
            if (ls.size() != p.ls.size())
                return false;
            for (auto i=ls.begin(), j=p.ls.begin(), n=ls.end(); i!=n; i++, j++)
                if (*i != *j)
                    return false;
            return true;
        }
        bool operator!=(Partial const& p) const { return !(*this == p); }
        friend inline std::ostream& operator<<(std::ostream& os, Partial const& p) {
            os << "[";
            bool first = true;
            for (auto i=p.ls.begin(), n=p.ls.end(); i!=n; i++) {
                if (first)
                    first = false;
                else
                    os << ", ";
                os << *i;
            }
            return os << "]";
        }
    };
    typedef _List Out;

    Out lower(const Partial& c) const {
        return c.ls;
    }

    Partial lift(const In& v) const {
        Partial part;
        part.ls.push_back(v);
        return part;
    }

    Partial combine(const Partial& a, const Partial& b) const {
        Partial part;
        part.ls.insert(part.ls.end(), a.ls.begin(), a.ls.end());
        part.ls.insert(part.ls.end(), b.ls.begin(), b.ls.end());
        return part;
    }

    Partial inverse_combine(const Partial& a, const Partial& b) const {
        Partial part;
        part.ls.insert(part.ls.end(), a.ls.begin(), a.ls.end());
        for (const In& item: b.ls) {
            part.ls.remove(item);
        }
        return part;
    }

    void recalc_combine(Partial& accum, const In& b) const {
        accum.ls.push_back(b);
    }

    std::string name() const { return "collect"; }

    static const Partial identity;
};

template <class In, class List>
const typename Collect<In, List>::Partial Collect<In, List>::identity = {List()};


template <class _In>
class MinCount {
public:
    typedef _In In;
    struct Partial {
        long n;
        In min;
        bool operator==(Partial const& p) const {
            return n==p.n && min==p.min;
        }
        bool operator!=(Partial const& p) const { return !(*this == p); }
        friend inline std::ostream& operator<<(std::ostream& os, Partial const& p) {
            return os << "{" << p.n << ", " << p.min << "}";
        }
    };
    typedef uint64_t Out;

    Out lower(const Partial& c) const {
        return c.n;
    }

    Partial lift(const In& v) const {
        Partial part;
        part.n = 1;
        part.min = v;
        return part;
    }

    Partial combine(const Partial& a, const Partial& b) const {
        if (a.n < 0) return b;
        if (b.n < 0) return a;
        if (a.min < b.min) {
            return a;
        }
        else if (a.min == b.min) {
            Partial part;
            part.n = a.n + b.n;
            part.min = a.min;
            return part;
        }
        return b;
    }

    void recalc_combine(Partial& accum, const In& b) const {
        if (b < accum.min || accum.n < 0) {
            accum.min = b;
            accum.n = 1;
        }
        else if (b == accum.min) {
            ++accum.n;
        }
    }

    std::string name() const { return "min-count"; }

    static const Partial identity;
};

template <class I>
const typename MinCount<I>::Partial MinCount<I>::identity = {-1, I()};

template <class _In, class _Partial=_In>
class BusyLoop {
public:
    typedef _In In;
    typedef _Partial Partial;
    typedef In Out;
    const size_t to_loop = 100;

    Out lower(const Partial& c) const {
        return static_cast<Out>(c);
    }

    Partial lift(const In& v) const {
        return static_cast<Partial>(v);
    }

    inline static double busy_looper(size_t num_loop) {
      double dummy = 1.0;
			for (size_t i=0;i<num_loop;++i) {
				dummy += 1.0/((double)((1+i)%1000000));
			}
			return dummy;
		}

    Partial combine(const Partial& a, const Partial& b) const {
      size_t num_loop = to_loop + (a+b)%11;
			double dummy = busy_looper(num_loop);
			int dummy_int = (int) dummy;
      return a + b + (dummy_int)%16;
    }

    Partial inverse_combine(const Partial& a, const Partial& b) const {
        return a - b;
    }

    void recalc_combine(Partial& accum, const In& b) const {
				size_t num_loop = to_loop + (accum + b)%11;
				double dummy = busy_looper(num_loop);
				int dummy_int = (int) dummy;
  
        accum += b + (dummy_int)%16;
    }

    std::string name() const { return "busy-loop"; }

    static const Partial identity;
};

template <class In, class Partial>
const typename BusyLoop<In, Partial>::Partial BusyLoop<In, Partial>::identity = In();

template <class _In, class _Out>
class RelativeVariation {
public:
    typedef _In In;
    struct Partial {
        int64_t min;
        int64_t max;
        bool operator==(Partial const& p) const {
            return min == p.min && max == p.max;
        }
        bool operator!=(Partial const& p) const { return !(*this == p); }

        friend inline std::ostream& operator<<(std::ostream& os, Partial const& p) {
            return os << "{" << p.min << ", " << p.max << "}";
        }
    };
    typedef _Out Out;

    Out lower(const Partial& c) const {
        return static_cast<Out>(c.max - c.min) / static_cast<Out>(c.max);
    }

    Partial lift(const In& v) const {
        Partial part;
        part.min = static_cast<int64_t>(v);
        part.max = static_cast<int64_t>(v);
        return part;
    }

    Partial combine(const Partial& a, const Partial& b) const {
        Partial part;

        if (a.min < b.min) {
            part.min = a.min;
        }
        else {
            part.min = b.min;
        }

        if (a.max > b.max) {
            part.max = a.max;
        }
        else {
            part.max = b.max;
        }

        return part;
    }

    void recalc_combine(Partial& accum, const In& b) const {
        if (b < accum.min) {
            accum.min = b;
        }
        if (b > accum.max) {
            accum.max = b;
        }
    }

    std::string name() const { return "relative-variation"; }

    static const Partial identity;
};

template <class In, class Out>
const typename RelativeVariation<In, Out>::Partial RelativeVariation<In, Out>::identity = {std::numeric_limits<int64_t>::min(), 
                                                                                           std::numeric_limits<int64_t>::max()};

#endif
