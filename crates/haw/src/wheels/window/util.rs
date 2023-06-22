#[derive(Debug, Clone, Copy)]
pub enum PairType {
    /// Slice size when range % slide = 0
    Even(usize),
    /// Two pairs (p1, p2) when range % slide != 0
    Uneven(usize, usize),
}

pub fn create_pair_type(range: usize, slide: usize) -> PairType {
    let p2 = range % slide;
    if p2 == 0 {
        PairType::Even(slide)
    } else {
        let p1 = slide - p2;
        PairType::Uneven(p1, p2)
    }
}
pub const fn panes_space(range: usize, slide: usize) -> usize {
    assert!(range >= slide, "Range needs to be larger than slide");
    range / gcd(range, slide)
}
pub const fn gcd(mut a: usize, mut b: usize) -> usize {
    while b != 0 {
        let temp = b;
        b = a % b;
        a = temp;
    }
    a
}

// Verifies that returned capacity is a power of two
pub const fn capacity(range: usize, slide: usize) -> usize {
    let space = panes_space(range, slide);
    if space.is_power_of_two() {
        space
    } else {
        space.next_power_of_two()
    }
}

#[inline]
const fn ceil_div(a: usize, b: usize) -> usize {
    (a + (b - 1)) / b
}

// Based on a Range and Slide, generate number of slots required using the Pairs technique
pub const fn pairs_space(range: usize, slide: usize) -> usize {
    assert!(range >= slide, "Range needs to be larger than slide");
    let p2 = range % slide;
    if p2 == 0 {
        range / slide
    } else {
        ceil_div(2 * range, slide)
    }
}

// Verifies that returned capacity is a power of two
pub const fn pairs_capacity(range: usize, slide: usize) -> usize {
    let space = pairs_space(range, slide);
    if space.is_power_of_two() {
        space
    } else {
        space.next_power_of_two()
    }
}
