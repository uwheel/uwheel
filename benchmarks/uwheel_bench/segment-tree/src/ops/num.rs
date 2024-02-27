#[cfg(feature = "num-bigint")]
mod bigint {

    use crate::ops::{Add, Commutative, Identity, Invertible, Mul, Operation};
    use num_bigint::{BigInt, BigUint};

    impl Operation<BigInt> for Add {
        /// Returns the sum. This usually allocates memory.
        fn combine(&self, a: &BigInt, b: &BigInt) -> BigInt {
            a + b
        }
        /// Computes the sum while reusing memory in `a`.
        fn combine_mut(&self, a: &mut BigInt, b: &BigInt) {
            *a += b;
        }
        /// Computes the sum while reusing memory in `b`.
        fn combine_mut2(&self, a: &BigInt, b: &mut BigInt) {
            *b += a;
        }
        /// Computes the sum while reusing memory from `a`.
        fn combine_left(&self, a: BigInt, b: &BigInt) -> BigInt {
            a + b
        }
        /// Computes the sum while reusing memory from `b`.
        fn combine_right(&self, a: &BigInt, b: BigInt) -> BigInt {
            a + b
        }
        /// Computes the sum while reusing memory from the larger of `a` and `b`.
        fn combine_both(&self, a: BigInt, b: BigInt) -> BigInt {
            a + b
        }
    }
    impl Commutative<BigInt> for Add {}
    impl Identity<BigInt> for Add {
        /// Returns zero.
        fn identity(&self) -> BigInt {
            0.into()
        }
    }
    impl Invertible<BigInt> for Add {
        /// Computes the difference, while reusing memory in `a`.
        fn uncombine(&self, a: &mut BigInt, b: &BigInt) {
            *a -= b;
        }
    }

    impl Operation<BigUint> for Add {
        /// Returns the sum. This usually allocates memory.
        fn combine(&self, a: &BigUint, b: &BigUint) -> BigUint {
            a + b
        }
        /// Computes the sum while reusing memory in `a`.
        fn combine_mut(&self, a: &mut BigUint, b: &BigUint) {
            *a += b;
        }
        /// Computes the sum while reusing memory in `b`.
        fn combine_mut2(&self, a: &BigUint, b: &mut BigUint) {
            *b += a;
        }
        /// Computes the sum while reusing memory from `a`.
        fn combine_left(&self, a: BigUint, b: &BigUint) -> BigUint {
            a + b
        }
        /// Computes the sum while reusing memory from `b`.
        fn combine_right(&self, a: &BigUint, b: BigUint) -> BigUint {
            a + b
        }
        /// Computes the sum while reusing memory from the larger of `a` and `b`.
        fn combine_both(&self, a: BigUint, b: BigUint) -> BigUint {
            a + b
        }
    }
    impl Commutative<BigUint> for Add {}
    impl Identity<BigUint> for Add {
        /// Returns zero.
        fn identity(&self) -> BigUint {
            0u32.into()
        }
    }

    impl Operation<BigInt> for Mul {
        /// Returns the product. This usually allocates memory.
        fn combine(&self, a: &BigInt, b: &BigInt) -> BigInt {
            a * b
        }
        /// Computes the product while reusing memory in `a`.
        fn combine_mut(&self, a: &mut BigInt, b: &BigInt) {
            *a *= b;
        }
        /// Computes the product while reusing memory in `b`.
        fn combine_mut2(&self, a: &BigInt, b: &mut BigInt) {
            *b *= a;
        }
        /// Computes the product while reusing memory from `a`.
        fn combine_left(&self, a: BigInt, b: &BigInt) -> BigInt {
            a * b
        }
        /// Computes the product while reusing memory from `b`.
        fn combine_right(&self, a: &BigInt, b: BigInt) -> BigInt {
            a * b
        }
        /// Computes the product while reusing memory from the larger of `a` and `b`.
        fn combine_both(&self, a: BigInt, b: BigInt) -> BigInt {
            a * b
        }
    }
    impl Commutative<BigInt> for Mul {}
    impl Identity<BigInt> for Mul {
        /// Returns one.
        fn identity(&self) -> BigInt {
            1.into()
        }
    }

    impl Operation<BigUint> for Mul {
        /// Returns the product. This usually allocates memory.
        fn combine(&self, a: &BigUint, b: &BigUint) -> BigUint {
            a * b
        }
        /// Computes the product while reusing memory in `a`.
        fn combine_mut(&self, a: &mut BigUint, b: &BigUint) {
            *a *= b;
        }
        /// Computes the product while reusing memory in `b`.
        fn combine_mut2(&self, a: &BigUint, b: &mut BigUint) {
            *b *= a;
        }
        /// Computes the product while reusing memory from `a`.
        fn combine_left(&self, a: BigUint, b: &BigUint) -> BigUint {
            a * b
        }
        /// Computes the product while reusing memory from `b`.
        fn combine_right(&self, a: &BigUint, b: BigUint) -> BigUint {
            a * b
        }
        /// Computes the product while reusing memory from the larger of `a` and `b`.
        fn combine_both(&self, a: BigUint, b: BigUint) -> BigUint {
            a * b
        }
    }
    impl Commutative<BigUint> for Mul {}
    impl Identity<BigUint> for Mul {
        /// Returns zero.
        fn identity(&self) -> BigUint {
            1u32.into()
        }
    }
}
