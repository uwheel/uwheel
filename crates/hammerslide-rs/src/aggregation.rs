#[derive(Clone, Copy)]
pub enum AggregationType {
    MIN,
    SUM,
    MAX,
    CNT,
    AVG,
}

// Define a trait for aggregation functions
pub trait Aggregation {
    type In; // Input type
    type Agg; // Aggregation type (intermediate)
    type Out; // Output type (final)

    // The identity value for the aggregation
    fn identity(&self) -> Self::Agg;

    // Lifts the input value to an aggregation type
    fn lift(&self, value: Self::In) -> Self::Agg;

    // Combines two aggregation values
    fn combine(&self, agg1: Self::Agg, agg2: Self::Agg) -> Self::Agg;

    // Lowers the aggregation result to the final output type
    fn lower(&self, agg: Self::Agg) -> Self::Out;
}
