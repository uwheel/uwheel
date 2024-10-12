/// A enum for representing the Pair type used for window slicing
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[derive(Debug, Clone, Copy)]
pub enum PairType {
    /// Slice size when range % slide = 0
    Even(usize),
    /// Two pairs (p1, p2) when range % slide != 0
    Uneven(usize, usize),
}
impl PairType {
    /// Returns `true` if the [PairType] is Uneven
    pub fn _is_uneven(&self) -> bool {
        matches!(self, PairType::Uneven { .. })
    }
}
/// Creates a new [PairType] from a given range and slide
pub fn create_pair_type(range: usize, slide: usize) -> PairType {
    let p1 = range % slide;
    if p1 == 0 {
        PairType::Even(slide)
    } else {
        let p2 = slide - p1;
        PairType::Uneven(p1, p2)
    }
}

/// Based on a Range and Slide, generate number of slots required using the Pairs technique
pub const fn pairs_space(range: usize, slide: usize) -> usize {
    assert!(range >= slide, "Range needs to be larger than slide");
    let p2 = range % slide;
    if p2 == 0 {
        range / slide
    } else {
        (2 * range).div_ceil(slide)
    }
}
