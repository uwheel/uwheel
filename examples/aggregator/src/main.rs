use awheel::{
    aggregator::{Aggregator, PartialAggregateType},
    time_internal::NumericalDuration,
    Entry,
    RwWheel,
};

/// Our user-defined Aggregator capturing the AVG wind speed and temperature of a sensor device.
#[derive(Clone, Debug, Default)]
pub struct CustomAggregator;

/// Raw input data from a sensor
#[derive(Clone, Debug, Copy)]
pub struct RawData {
    wind_speed: f64,
    temperature: f64,
}

/// Partial Aggregate used (sum + count) in order to calculate AVG
#[derive(Clone, PartialEq, Debug, Default, Copy)]
#[repr(C)]
pub struct PartialAvg {
    count: f64,
    sum: f64,
}
impl PartialAvg {
    #[inline]
    fn update(&mut self, sum: f64) {
        self.sum += sum;
        self.count += 1.0;
    }
    fn avg(&self) -> f64 {
        self.sum / self.count
    }
}

/// Partial Aggregate State for 2 attributes (Wind speed + Temperature)
#[derive(Clone, PartialEq, Default, Debug, Copy)]
#[repr(C)]
pub struct PartialAggregate {
    wind_speed: PartialAvg,
    temperature: PartialAvg,
}

impl PartialAggregate {
    pub fn new(raw: RawData) -> Self {
        Self {
            wind_speed: PartialAvg {
                count: 1.0,
                sum: raw.wind_speed,
            },
            temperature: PartialAvg {
                count: 1.0,
                sum: raw.temperature,
            },
        }
    }
    /// Updates the partials using the raw data
    #[inline]
    pub fn insert(&mut self, raw: RawData) {
        self.wind_speed.update(raw.wind_speed);
        self.temperature.update(raw.temperature);
    }

    /// Merges another instance into this PartialAggregate
    #[inline]
    pub fn merge(&mut self, other: Self) {
        let other_temp_sum = other.temperature.sum;
        let other_temp_count = other.temperature.count;

        // Merge temperature partials
        self.temperature = PartialAvg {
            count: self.temperature.count + other_temp_count,
            sum: self.temperature.sum + other_temp_sum,
        };

        let other_wind_speed_sum = other.wind_speed.sum;
        let other_wind_speed_count = other.wind_speed.count;

        // Merge wind speed partials
        self.wind_speed = PartialAvg {
            count: self.wind_speed.count + other_wind_speed_count,
            sum: self.wind_speed.sum + other_wind_speed_sum,
        };
    }
    pub const fn identity() -> Self {
        Self {
            wind_speed: PartialAvg {
                count: 0.0,
                sum: 0.0,
            },
            temperature: PartialAvg {
                count: 0.0,
                sum: 0.0,
            },
        }
    }
}

// Need to implement PartialAggregateType for our custom struct
impl PartialAggregateType for PartialAggregate {}

/// Lowered Aggregate State
#[derive(Debug, Copy, Clone)]
pub struct Aggregate {
    pub avg_wind_speed: f64,
    pub avg_temperature: f64,
}
impl Aggregate {
    pub fn from(partial: PartialAggregate) -> Self {
        Self {
            avg_wind_speed: partial.wind_speed.avg(),
            avg_temperature: partial.temperature.avg(),
        }
    }
}

// Implement the Aggregator trait for our aggregator
// NOTE: in this case both the mutable and immutable aggregate types are the same
impl Aggregator for CustomAggregator {
    const IDENTITY: Self::PartialAggregate = PartialAggregate::identity();
    type CombineSimd = fn(&[Self::PartialAggregate]) -> Self::PartialAggregate;

    type CombineInverse =
        fn(Self::PartialAggregate, Self::PartialAggregate) -> Self::PartialAggregate;

    type Input = RawData;
    type PartialAggregate = PartialAggregate;
    type MutablePartialAggregate = PartialAggregate;
    type Aggregate = Aggregate;

    // Lift data into a Mutable aggregate if none exists
    fn lift(input: Self::Input) -> Self::MutablePartialAggregate {
        PartialAggregate::new(input)
    }
    // Update the mutable aggregate above the watermark
    fn combine_mutable(mutable: &mut Self::MutablePartialAggregate, input: Self::Input) {
        mutable.insert(input);
    }
    // freeze the mutable aggregate to immutable
    // NOTE: in this case the types are the same
    fn freeze(a: Self::MutablePartialAggregate) -> Self::PartialAggregate {
        a
    }

    // combine two immutable partial aggregates
    fn combine(mut a: Self::PartialAggregate, b: Self::PartialAggregate) -> Self::PartialAggregate {
        a.merge(b);
        a
    }
    // lower partial agg to a final aggregate result
    fn lower(a: Self::PartialAggregate) -> Self::Aggregate {
        Aggregate::from(a)
    }
}

fn main() {
    // using 0 as start time
    let start_time = 0;
    let mut wheel: RwWheel<CustomAggregator> = RwWheel::new(start_time);

    // Insert some data
    wheel.insert(Entry::new(
        RawData {
            wind_speed: 50.0,
            temperature: 100.0,
        },
        0,
    ));
    wheel.insert(Entry::new(
        RawData {
            wind_speed: 30.0,
            temperature: 55.0,
        },
        1000,
    ));
    wheel.insert(Entry::new(
        RawData {
            wind_speed: 29.0,
            temperature: 65.0,
        },
        2000,
    ));

    // advance the wheel by 3 seconds
    wheel.advance(3.seconds());

    // verify the partial aggregates and also lowered aggregate type

    let partial_3_sec = wheel.read().interval(3.seconds()).unwrap();
    dbg!(partial_3_sec);

    assert_eq!(
        partial_3_sec,
        PartialAggregate {
            wind_speed: PartialAvg {
                count: 3.0,
                sum: 109.0
            },
            temperature: PartialAvg {
                count: 3.0,
                sum: 220.0
            },
        }
    );

    let aggregate_3_sec = wheel.read().interval_and_lower(3.seconds()).unwrap();
    dbg!(aggregate_3_sec);
    let float_fmt = |f: f64| format!("{:.2}", f);

    assert_eq!(float_fmt(aggregate_3_sec.avg_wind_speed), "36.33");
    assert_eq!(float_fmt(aggregate_3_sec.avg_temperature), "73.33");
}
