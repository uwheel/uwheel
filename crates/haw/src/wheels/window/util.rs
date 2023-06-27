#[derive(Debug, Clone, Copy)]
pub enum PairType {
    /// Slice size when range % slide = 0
    Even(usize),
    /// Two pairs (p1, p2) when range % slide != 0
    Uneven(usize, usize),
}
impl PairType {
    pub fn is_uneven(&self) -> bool {
        matches!(self, PairType::Uneven { .. })
    }
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
