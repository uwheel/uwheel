use parking_lot::RwLock;
use rocksdb::{merge_operator::MergeFn, Options, DB};
use std::{cell::RefCell, rc::Rc};

mod wheel;
use wheel::LSMWheel;

const SECONDS_DIMENSION: &str = "seconds";
const MINUTES_DIMENSION: &str = "minutes";
const HOURS_DIMENSION: &str = "hours";
const DAYS_DIMENSION: &str = "days";
const WEEKS_DIMENSION: &str = "weeks";
const MONTHS_DIMENSION: &str = "months";
const YEARS_DIMENSION: &str = "years";

const SECONDS: usize = 60;
const MINUTES: usize = 60;
const HOURS: usize = 24;
const DAYS: usize = 7;
const WEEKS: usize = 4;
const MONTHS: usize = 12;

pub const YEARS: usize = 10;

// Default Wheel Capacities (power of two)
const SECONDS_CAP: usize = 128; // This is set as 128 instead of 64 to support write ahead slots
const MINUTES_CAP: usize = 64;
const HOURS_CAP: usize = 32;
const DAYS_CAP: usize = 8;
const WEEKS_CAP: usize = 8;
const MONTHS_CAP: usize = 16;
const YEARS_CAP: usize = YEARS.next_power_of_two();

/*
use haw::types::PartialAggregateType;
pub fn to_merge_fnz<'a, P: PartialAggregateType>(
    combine_fn: impl Fn(P, P) -> P,
) -> impl for<'b> Fn(&'b [u8], Option<&'a [u8]>, &'a MergeOperands) -> Vec<u8>
where
    <<P as PartialAggregateType>::Bytes as TryFrom<&'a [u8]>>::Error: std::fmt::Debug,
{
    let sum_merge =
        move |_: &[u8], existing_val: Option<&'a [u8]>, operands: &'a MergeOperands| -> Vec<u8> {
            let mut partial_agg: P = existing_val
                .map(|v| P::from_le_bytes(v.try_into().unwrap()))
                .unwrap_or(Default::default());

            for operand in operands.iter() {
                let agg = P::from_le_bytes(operand.try_into().unwrap());
                partial_agg = combine_fn(partial_agg, agg);
            }
            partial_agg.to_le_bytes().as_ref().to_vec()
        };
    sum_merge
}
*/

pub struct Wheel {
    watermark: RwLock<u64>,
    //_star_wheel: RwLock<haw::Wheel<A>>,
    seconds_wheel: RwLock<LSMWheel>,
    minutes_wheel: RwLock<LSMWheel>,
    hours_wheel: RwLock<LSMWheel>,
    days_wheel: RwLock<LSMWheel>,
    weeks_wheel: RwLock<LSMWheel>,
    months_wheel: RwLock<LSMWheel>,
    years_wheel: RwLock<LSMWheel>,
}
impl Wheel {
    const SECOND_AS_MS: u64 = time::Duration::SECOND.whole_milliseconds() as u64;
    const MINUTES_AS_SECS: u64 = time::Duration::MINUTE.whole_seconds() as u64;
    const HOURS_AS_SECS: u64 = time::Duration::HOUR.whole_seconds() as u64;
    const DAYS_AS_SECS: u64 = time::Duration::DAY.whole_seconds() as u64;
    const WEEK_AS_SECS: u64 = time::Duration::WEEK.whole_seconds() as u64;
    const MONTH_AS_SECS: u64 = Self::WEEK_AS_SECS * WEEKS as u64;
    const YEAR_AS_SECS: u64 = Self::MONTH_AS_SECS * MONTHS as u64;

    const TOTAL_SECS_IN_WHEEL: u64 = Self::YEAR_AS_SECS * YEARS as u64;
    pub const CYCLE_LENGTH: time::Duration =
        time::Duration::seconds((Self::YEAR_AS_SECS * (YEARS as u64 + 1)) as i64); // need 1 extra to force full cycle rotation
    pub const TOTAL_WHEEL_SLOTS: usize = SECONDS + MINUTES + HOURS + DAYS + WEEKS + MONTHS + YEARS;
    pub const MAX_WRITE_AHEAD_SLOTS: usize = SECONDS_CAP - SECONDS;

    pub fn create_column_family(db: &Rc<RefCell<DB>>, cf_name: impl AsRef<str>, opts: &Options) {
        if db.borrow().cf_handle(cf_name.as_ref()).is_none() {
            db.borrow_mut().create_cf(cf_name, opts).unwrap();
        }
    }

    pub fn new(cf: &str, db: Rc<RefCell<DB>>, time: u64, merge_fn: impl MergeFn + Clone) -> Self {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_merge_operator_associative("wheel", merge_fn);

        let cf_fmt = |cf: &str, dim: &str| format!("{}_{}", cf, dim);

        let seconds_cf = cf_fmt(cf, SECONDS_DIMENSION);
        let minutes_cf = cf_fmt(cf, MINUTES_DIMENSION);
        let hours_cf = cf_fmt(cf, HOURS_DIMENSION);
        let days_cf = cf_fmt(cf, DAYS_DIMENSION);
        let weeks_cf = cf_fmt(cf, WEEKS_DIMENSION);
        let months_cf = cf_fmt(cf, MONTHS_DIMENSION);
        let years_cf = cf_fmt(cf, YEARS_DIMENSION);

        Self::create_column_family(&db, &seconds_cf, &opts);
        Self::create_column_family(&db, &minutes_cf, &opts);
        Self::create_column_family(&db, &hours_cf, &opts);
        Self::create_column_family(&db, &days_cf, &opts);
        Self::create_column_family(&db, &weeks_cf, &opts);
        Self::create_column_family(&db, &months_cf, &opts);
        Self::create_column_family(&db, &years_cf, &opts);

        Self {
            seconds_wheel: RwLock::new(LSMWheel::new(SECONDS, SECONDS_CAP, db.clone(), seconds_cf)),
            minutes_wheel: RwLock::new(LSMWheel::new(MINUTES, MINUTES_CAP, db.clone(), minutes_cf)),
            hours_wheel: RwLock::new(LSMWheel::new(HOURS, HOURS_CAP, db.clone(), hours_cf)),
            days_wheel: RwLock::new(LSMWheel::new(DAYS, DAYS_CAP, db.clone(), days_cf)),
            weeks_wheel: RwLock::new(LSMWheel::new(WEEKS, WEEKS_CAP, db.clone(), weeks_cf)),
            months_wheel: RwLock::new(LSMWheel::new(MONTHS, MONTHS_CAP, db.clone(), months_cf)),
            years_wheel: RwLock::new(LSMWheel::new(YEARS, YEARS_CAP, db, years_cf)),
            watermark: RwLock::new(time),
        }
    }

    /// Returns how many wheel slots are utilised
    pub fn len(&self) -> usize {
        self.seconds_wheel.read().len()
            + self.minutes_wheel.read().len()
            + self.hours_wheel.read().len()
            + self.days_wheel.read().len()
            + self.weeks_wheel.read().len()
            + self.months_wheel.read().len()
            + self.years_wheel.read().len()
    }

    /// Returns true if the internal wheel time has never been advanced
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns true if all slots in the hierarchy are utilised
    pub fn is_full(&self) -> bool {
        self.len() == Self::TOTAL_WHEEL_SLOTS
    }

    /// Returns how many slots (seconds) in front of the current watermark that can be written to
    pub fn write_ahead_len(&self) -> usize {
        self.seconds_wheel.read().write_ahead_len()
    }

    /// Returns how many ticks (seconds) are left until the wheel is fully utilised
    pub fn remaining_ticks(&self) -> u64 {
        Self::TOTAL_SECS_IN_WHEEL - self.current_time_in_cycle().whole_seconds() as u64
    }

    /// Returns Duration that represents where the wheel currently is in its cycle
    #[inline]
    pub fn current_time_in_cycle(&self) -> time::Duration {
        let secs = self.seconds_wheel.read().rotation_count() as u64;
        let min_secs = self.minutes_wheel.read().rotation_count() as u64 * Self::MINUTES_AS_SECS;
        let hr_secs = self.hours_wheel.read().rotation_count() as u64 * Self::HOURS_AS_SECS;
        let day_secs = self.days_wheel.read().rotation_count() as u64 * Self::DAYS_AS_SECS;
        let week_secs = self.weeks_wheel.read().rotation_count() as u64 * Self::WEEK_AS_SECS;
        let month_secs = self.months_wheel.read().rotation_count() as u64 * Self::MONTH_AS_SECS;
        let year_secs = self.years_wheel.read().rotation_count() as u64 * Self::YEAR_AS_SECS;
        let cycle_time = secs + min_secs + hr_secs + day_secs + week_secs + month_secs + year_secs;
        time::Duration::seconds(cycle_time as i64)
    }
    pub fn seconds_wheel(&self) -> &RwLock<LSMWheel> {
        &self.seconds_wheel
    }

    /// Advance the watermark of the wheel by the given [time::Duration]
    #[inline]
    pub fn advance(&self, duration: time::Duration) {
        let ticks: usize = duration.whole_seconds() as usize;

        // helper fn to tick N times
        let tick_n = |ticks: usize, wheel: &Self| {
            for _ in 0..ticks {
                wheel.tick();
            }
        };

        tick_n(ticks, self);
    }
    /// Advances the time of the wheel aligned by the lowest unit (Second)
    #[inline]
    pub fn advance_to(&self, watermark: u64) {
        let diff = watermark.saturating_sub(self.watermark());
        self.advance(time::Duration::milliseconds(diff as i64));
    }

    pub fn clear(&mut self) {}

    /// Inserts entry into the wheel
    #[inline]
    pub fn insert(&self, key: impl AsRef<[u8]>, entry: impl AsRef<[u8]>, timestamp: u64) {
        let watermark = self.watermark();

        // If timestamp is below the watermark, then reject it.
        if timestamp < watermark {
            //Err(Error::Late { entry, watermark })
        } else {
            let diff = timestamp - watermark;
            let seconds = std::time::Duration::from_millis(diff).as_secs();

            if self.seconds_wheel.read().can_write_ahead(seconds) {
                self.seconds_wheel
                    .read()
                    .write_ahead(seconds as usize, key, entry);
            } else {
                /*
                // cannot fit within the wheel, return it to the user to handle it..
                let write_ahead_ms = Duration::from_secs(self.write_ahead_len() as u64).as_millis();
                let max_write_ahead_ts = self.watermark + write_ahead_ms as u64;
                Err(Error::Overflow {
                    entry,
                    max_write_ahead_ts,
                })
                */
            }
        }
    }
    /// Tick the wheel by a single unit (second)
    ///
    /// In the worst case, a tick may cause a rotation of all the wheels in the hierarchy.
    #[inline]
    fn tick(&self) {
        *self.watermark.write() += Self::SECOND_AS_MS;

        // full rotation of seconds wheel
        if let Some(_rot_data) = self.seconds_wheel.write().tick() {
            //
        }
    }

    /// Return the current watermark as milliseconds for this wheel
    #[inline]
    pub fn watermark(&self) -> u64 {
        *self.watermark.read()
    }
}
