use std::{
    cell::{Cell, RefCell},
    collections::VecDeque,
    rc::Rc,
};

use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use eframe::egui::{self};
use egui::{Color32, Response, RichText, ScrollArea, Ui};
use egui_extras::DatePickerButton;
use egui_plot::{Bar, BarChart, Legend, Plot, PlotPoint};
use hdrhistogram::Histogram;
use postcard::to_allocvec;
use time::OffsetDateTime;
use uwheel::{
    aggregator::sum::U64SumAggregator,
    wheels::read::{aggregation::conf::RetentionPolicy, hierarchical::HawConf, Haw},
    Conf,
    Entry,
    NumericalDuration,
    RwWheel,
    WheelRange,
};

thread_local! {
    pub static QUERY_LATENCY: RefCell<Histogram<u64>> = RefCell::new(Histogram::new(4).unwrap());
}

#[inline]
fn measure<T>(query: impl Fn() -> T) -> T {
    // When running natively
    #[cfg(not(target_arch = "wasm32"))]
    {
        let now = std::time::Instant::now();
        let res = query();
        let elapsed = now.elapsed();
        QUERY_LATENCY.with(|hist| {
            hist.borrow_mut()
                .record(elapsed.as_micros() as u64)
                .unwrap();
        });

        res
    }
    #[cfg(target_arch = "wasm32")]
    query()
}

fn to_offset_datetime(time_ms: u64) -> OffsetDateTime {
    // time represented in milliseconds, convert it to seconds for `OffsetDateTime`
    let unix_ts = time_ms.saturating_div(1000);
    OffsetDateTime::from_unix_timestamp(unix_ts as i64).unwrap()
}

pub const WRITE_AHEAD_COLOR: Color32 = Color32::from_rgb(67, 110, 3);
pub const WATERMARK_COLOR: Color32 = Color32::from_rgb(0, 255, 255);
pub const SECOND_COLOR: Color32 = Color32::from_rgb(247, 71, 55);
pub const MINUTE_COLOR: Color32 = Color32::from_rgb(38, 118, 199);
pub const HOUR_COLOR: Color32 = Color32::from_rgb(0, 218, 0);
pub const DAY_COLOR: Color32 = Color32::from_rgb(222, 0, 204);
pub const WEEK_COLOR: Color32 = Color32::from_rgb(255, 237, 73);
pub const YEAR_COLOR: Color32 = Color32::from_rgb(255, 143, 154);

#[derive(Clone, Copy, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum Granularity {
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Year,
}
impl Default for Granularity {
    fn default() -> Self {
        Self::Second
    }
}

fn calculate_granularity(position: usize) -> Option<(Granularity, usize)> {
    if (1..=59).contains(&position) {
        Some((Granularity::Second, position))
    } else if (60..=120).contains(&position) {
        Some((Granularity::Minute, position - 60))
    } else if (121..=144).contains(&position) {
        Some((Granularity::Hour, position - 120))
    } else if (145..=151).contains(&position) {
        Some((Granularity::Day, position - 144))
    } else if (152..=167).contains(&position) {
        Some((Granularity::Week, position - 151))
    } else if (168..=177).contains(&position) {
        Some((Granularity::Year, position - 167))
    } else {
        None
    }
}

pub type DemoAggregator = U64SumAggregator;

#[derive(serde::Deserialize, serde::Serialize)]
pub struct HawLabels {
    pub(crate) watermark_unix_label: String,
    pub(crate) watermark_label: String,
    pub(crate) slots_len_label: String,
    pub(crate) landmark_window_label: String,
    pub(crate) remaining_ticks_label: String,
    pub(crate) seconds_ticks_label: String,
    pub(crate) minutes_ticks_label: String,
    pub(crate) hours_ticks_label: String,
    pub(crate) days_ticks_label: String,
    pub(crate) weeks_ticks_label: String,
    pub(crate) years_ticks_label: String,
}
impl HawLabels {
    pub fn new(wheel: &RwWheel<DemoAggregator>) -> Self {
        let watermark_unix_label = wheel.read().watermark().to_string();
        let watermark_label = to_offset_datetime(wheel.read().watermark()).to_string();
        let slots_len_label = wheel.read().len().to_string();
        let remaining_ticks_label = wheel.read().remaining_ticks().to_string();
        let seconds_ticks_label = wheel
            .read()
            .as_ref()
            .seconds()
            .map(|w| w.ticks_remaining().to_string())
            .unwrap_or_else(|| "None".to_string());
        let minutes_ticks_label = wheel
            .read()
            .as_ref()
            .minutes()
            .map(|w| w.ticks_remaining().to_string())
            .unwrap_or_else(|| "None".to_string());
        let hours_ticks_label = wheel
            .read()
            .as_ref()
            .hours()
            .map(|w| w.ticks_remaining().to_string())
            .unwrap_or_else(|| "None".to_string());
        let days_ticks_label = wheel
            .read()
            .as_ref()
            .days()
            .map(|w| w.ticks_remaining().to_string())
            .unwrap_or_else(|| "None".to_string());
        let weeks_ticks_label = wheel
            .read()
            .as_ref()
            .weeks()
            .map(|w| w.ticks_remaining().to_string())
            .unwrap_or_else(|| "None".to_string());
        let years_ticks_label = wheel
            .read()
            .as_ref()
            .years()
            .map(|w| w.ticks_remaining().to_string())
            .unwrap_or_else(|| "None".to_string());
        let landmark_window_label = wheel.read().landmark().unwrap_or(0).to_string();
        Self {
            watermark_unix_label,
            watermark_label,
            slots_len_label,
            remaining_ticks_label,
            seconds_ticks_label,
            minutes_ticks_label,
            hours_ticks_label,
            days_ticks_label,
            weeks_ticks_label,
            years_ticks_label,
            landmark_window_label,
        }
    }
}

/// We derive Deserialize/Serialize so we can persist app state on shutdown.
#[derive(serde::Deserialize, serde::Serialize)]
#[serde(default)] // if we add new fields, give them default values when deserializing old state
pub struct TemplateApp {
    #[serde(skip)]
    labels: HawLabels,
    log: VecDeque<LogEntry>,
    tick_granularity: Granularity,
    //#[serde(skip)]
    //wheel: Rc<RefCell<Wheel<DemoAggregator>>>,
    #[serde(skip)]
    star_wheel: Rc<RefCell<RwWheel<DemoAggregator>>>,
    timestamp: String,
    aggregate: String,
    ticks: u64,
    encoded_bytes_len: usize,
    compressed_bytes_len: usize,
    about: About,
    about_open: Cell<bool>,
    #[serde(skip)]
    insert_date: Option<NaiveDate>,
    insert_time: String,
    #[serde(skip)]
    start_date: Option<NaiveDate>,
    start_time: String,
    #[serde(skip)]
    end_date: Option<NaiveDate>,
    end_time: String,
    query_result: String,
    explain_query: bool,
}

fn build_wheel(watermark: u64) -> RwWheel<DemoAggregator> {
    let mut conf = HawConf::default().with_watermark(watermark);
    conf.seconds.set_retention_policy(RetentionPolicy::Keep);

    conf.minutes.set_retention_policy(RetentionPolicy::Keep);

    conf.hours.set_retention_policy(RetentionPolicy::Keep);

    conf.days.set_retention_policy(RetentionPolicy::Keep);

    conf.weeks.set_retention_policy(RetentionPolicy::Keep);

    conf.years.set_retention_policy(RetentionPolicy::Keep);

    let conf = Conf::default().with_haw_conf(conf);
    RwWheel::with_conf(conf)
}

impl Default for TemplateApp {
    #[allow(clippy::redundant_closure)]
    fn default() -> Self {
        let watermark_date = chrono::Local::now().naive_local();
        let watermark_date = watermark_date.with_second(0).unwrap();
        let mut watermark_ms = watermark_date.and_utc().timestamp_millis() as u64;
        watermark_ms = align_timestamp_round(watermark_ms as i64) as u64;
        fn align_timestamp_round(timestamp_ms: i64) -> i64 {
            // Add 500 to bias towards rounding up for values exactly in between
            (timestamp_ms) / 1000 * 1000
        }

        let wheel = build_wheel(watermark_ms);

        let labels = HawLabels::new(&wheel);
        Self {
            star_wheel: Rc::new(RefCell::new(wheel)),
            labels,
            tick_granularity: Default::default(),
            log: Default::default(),
            timestamp: "1000".to_owned(),
            aggregate: "1".to_owned(),
            ticks: 1,
            encoded_bytes_len: 0,
            compressed_bytes_len: 0,
            about: Default::default(),
            about_open: Cell::new(true),
            insert_date: None,
            insert_time: watermark_date.time().format("%H:%M:%S").to_string(),
            start_date: None,
            start_time: "00:00:00".to_string(),
            end_date: None,
            end_time: "00:00:00".to_string(),
            query_result: "".to_owned(),
            explain_query: false,
        }
    }
}

impl TemplateApp {
    /// Called once before the first frame.
    pub fn new(_cc: &eframe::CreationContext<'_>) -> Self {
        Default::default()
    }
    // TODO: optimise
    fn wheels_plot(&self, wheel: Rc<RefCell<RwWheel<DemoAggregator>>>, ui: &mut Ui) -> Response {
        let fmt_str = |i: usize, gran: Granularity| -> String { format!("{} {:?} ago", i, gran) };

        // Write-ahead chart
        let mut pos: f64 = 1.0;
        let mut bars = Vec::new();

        for (y_pos, i) in (0..63).enumerate() {
            let val = *wheel.borrow_mut().write().at(i + 1).unwrap_or(&0) as f64;
            let bar = Bar::new(pos, val).name(format!("{} seconds ahead", y_pos + 1));
            pos += 1.0;
            bars.push(bar);
        }
        let write_ahead_chart = BarChart::new(bars)
            .highlight(true)
            .color(WRITE_AHEAD_COLOR)
            .width(0.7)
            .name("Write-ahead");

        // Watermark
        pos = 0.0;
        let watermark = wheel.borrow().watermark();
        let watermark_agg = *wheel.borrow_mut().write().at(0).unwrap_or(&0) as f64;
        let bar = Bar::new(pos, watermark_agg).name(to_offset_datetime(watermark).to_string());
        let watermark_chart = BarChart::new(vec![bar])
            .highlight(true)
            .color(WATERMARK_COLOR)
            .width(0.7)
            .name("Watermark");

        let wheel = wheel.borrow();

        // in order to start below the watermark
        pos -= 1.0;

        let mut bars = Vec::new();
        if let Some(seconds_wheel) = wheel.read().as_ref().seconds() {
            for i in 0..=uwheel::SECONDS - 1 {
                let val = seconds_wheel.lower_at(i).unwrap_or(0) as f64;
                let bar = Bar::new(pos, val).name(fmt_str(i + 1, Granularity::Second));
                pos -= 1.0;
                bars.push(bar);
            }
        }
        let seconds_chart = BarChart::new(bars)
            .highlight(true)
            .color(SECOND_COLOR)
            .width(0.7)
            .name("Seconds");

        // MINUTES
        let mut bars = Vec::new();
        if let Some(minutes_wheel) = wheel.read().as_ref().minutes() {
            for i in 0..=uwheel::MINUTES - 1 {
                let val = minutes_wheel.lower_at(i).unwrap_or(0) as f64;
                let bar = Bar::new(pos, val).name(fmt_str(i + 1, Granularity::Minute));
                pos -= 1.0;
                bars.push(bar);
            }
        }
        let minutes_chart = BarChart::new(bars)
            .width(0.7)
            .color(MINUTE_COLOR)
            .highlight(true)
            .name("Minutes");

        // HOURS
        let mut bars = Vec::new();
        if let Some(hours_wheel) = wheel.read().as_ref().hours() {
            for i in 0..=uwheel::HOURS - 1 {
                let val = hours_wheel.lower_at(i).unwrap_or(0) as f64;
                let bar = Bar::new(pos, val).name(fmt_str(i + 1, Granularity::Hour));
                pos -= 1.0;
                bars.push(bar);
            }
        }
        let hours_chart = BarChart::new(bars)
            .width(0.7)
            .highlight(true)
            .color(HOUR_COLOR)
            .name("Hours");

        let mut bars = Vec::new();
        if let Some(days_wheel) = wheel.read().as_ref().days() {
            for i in 0..=uwheel::DAYS - 1 {
                let val = days_wheel.lower_at(i).unwrap_or(0) as f64;
                let bar = Bar::new(pos, val).name(fmt_str(i + 1, Granularity::Day));
                pos -= 1.0;
                bars.push(bar);
            }
        }
        let days_chart = BarChart::new(bars)
            .width(0.7)
            .highlight(true)
            .color(DAY_COLOR)
            .name("Days");

        let mut bars = Vec::new();
        if let Some(weeks_wheel) = wheel.read().as_ref().weeks() {
            for i in 0..=uwheel::WEEKS - 1 {
                let val = weeks_wheel.lower_at(i).unwrap_or(0) as f64;
                let bar = Bar::new(pos, val).name(fmt_str(i + 1, Granularity::Week));
                pos -= 1.0;
                bars.push(bar);
            }
        }
        let weeks_chart = BarChart::new(bars)
            .width(0.7)
            .highlight(true)
            .color(WEEK_COLOR)
            .name("Weeks");

        let mut bars = Vec::new();
        if let Some(years_wheel) = wheel.read().as_ref().years() {
            for i in 0..=uwheel::YEARS - 1 {
                let val = years_wheel.lower_at(i).unwrap_or(0) as f64;
                let bar = Bar::new(pos, val).name(fmt_str(i + 1, Granularity::Year));
                pos -= 1.0;
                bars.push(bar);
            }
        }
        let years_chart = BarChart::new(bars)
            .width(0.7)
            .highlight(true)
            .color(YEAR_COLOR)
            .name("Years");
        // let empty_plot = |ui: &mut Ui| {
        //     Plot::new("Drill down")
        //         .legend(Legend::default())
        //         .data_aspect(1.0)
        //         .show(ui, |_plot_ui| {})
        //         .response
        // };
        let watermark_date = to_offset_datetime(wheel.read().watermark());

        let label_fmt = move |_s: &str, val: &PlotPoint| {
            let x = val.x as usize;
            if x == 0 {
                watermark_date.to_string()
            } else {
                match calculate_granularity(x) {
                    Some((gran, pos)) => {
                        format!("{} {:?} ago", pos, gran)
                    }
                    None => String::new(),
                }
            }
        };

        Plot::new("uwheel")
            .legend(Legend::default())
            .data_aspect(1.0)
            .label_formatter(label_fmt)
            .show(ui, |plot_ui| {
                plot_ui.bar_chart(write_ahead_chart);
                plot_ui.bar_chart(watermark_chart);
                plot_ui.bar_chart(seconds_chart);
                plot_ui.bar_chart(minutes_chart);
                plot_ui.bar_chart(hours_chart);
                plot_ui.bar_chart(days_chart);
                plot_ui.bar_chart(weeks_chart);
                plot_ui.bar_chart(years_chart);
                // NOTE: disable drill-dpwn for now as functionality is not there (can add using range query instead..)
                // if let Some(pos) = plot_ui.ctx().pointer_hover_pos() {
                //     if pos.x > 0.0 {
                //         egui::Window::new("Drill Down").default_width(620.0).show(
                //             plot_ui.ctx(),
                //             |ui| {
                //                 let p = plot_ui.plot_from_screen(pos);
                //                 let x_pos = p.x.clamp(0.0, 1000.0);
                //                 let slot = x_pos.floor() as usize;
                //                 match calculate_granularity(slot) {
                //                     Some((Granularity::Second, _)) => {
                //                         // cannot drill down seconds
                //                         empty_plot(ui);
                //                     }
                //                     Some((Granularity::Minute, pos)) => {
                //                         if let Some(minutes) = wheel.read().as_ref().minutes() {
                //                             if let Some(slots) = measure(|| minutes.drill_down(pos))
                //                             {
                //                                 let mut bars = Vec::new();
                //                                 let mut pos = 0.5;
                //                                 for (i, s) in slots.iter().enumerate() {
                //                                     let bar = Bar::new(pos, *s as f64)
                //                                         .name(fmt_str(i, Granularity::Second));
                //                                     pos += 1.0;
                //                                     bars.push(bar);
                //                                 }
                //                                 let seconds_chart = BarChart::new(bars)
                //                                     .width(0.7)
                //                                     .color(SECOND_COLOR)
                //                                     .highlight(true)
                //                                     .name("Seconds");
                //                                 Plot::new("Drill down")
                //                                     .legend(Legend::default())
                //                                     .auto_bounds_y()
                //                                     .auto_bounds_x()
                //                                     .data_aspect(1.0)
                //                                     .show(ui, |plot_ui| {
                //                                         plot_ui.bar_chart(seconds_chart);
                //                                     });
                //                             }
                //                         }
                //                     }
                //                     Some((Granularity::Hour, pos)) => {
                //                         if let Some(hours) = wheel.read().as_ref().hours() {
                //                             if let Some(slots) = measure(|| hours.drill_down(pos)) {
                //                                 let mut bars = Vec::new();
                //                                 let mut pos = 0.5;
                //                                 for (i, s) in slots.iter().enumerate() {
                //                                     let bar = Bar::new(pos, *s as f64)
                //                                         .name(fmt_str(i, Granularity::Minute));
                //                                     pos += 1.0;
                //                                     bars.push(bar);
                //                                 }
                //                                 let minutes_chart = BarChart::new(bars)
                //                                     .width(0.7)
                //                                     .color(MINUTE_COLOR)
                //                                     .highlight(true)
                //                                     .name("Minutes");
                //                                 Plot::new("Drill down")
                //                                     .legend(Legend::default())
                //                                     .auto_bounds_y()
                //                                     .auto_bounds_x()
                //                                     .data_aspect(1.0)
                //                                     .show(ui, |plot_ui| {
                //                                         plot_ui.bar_chart(minutes_chart);
                //                                     });
                //                             }
                //                         }
                //                     }
                //                     Some((Granularity::Day, pos)) => {
                //                         if let Some(days) = wheel.read().as_ref().days() {
                //                             if let Some(slots) = measure(|| days.drill_down(pos)) {
                //                                 let mut bars = Vec::new();
                //                                 let mut pos = 0.5;
                //                                 for (i, s) in slots.iter().enumerate() {
                //                                     let bar = Bar::new(pos, *s as f64)
                //                                         .name(fmt_str(i, Granularity::Hour));
                //                                     pos += 1.0;
                //                                     bars.push(bar);
                //                                 }
                //                                 let hours_chart = BarChart::new(bars)
                //                                     .width(0.7)
                //                                     .color(HOUR_COLOR)
                //                                     .highlight(true)
                //                                     .name("Hours");
                //                                 Plot::new("Drill down")
                //                                     .legend(Legend::default())
                //                                     .auto_bounds_y()
                //                                     .data_aspect(1.0)
                //                                     .show(ui, |plot_ui| {
                //                                         plot_ui.bar_chart(hours_chart);
                //                                     });
                //                             }
                //                         }
                //                     }
                //                     Some((Granularity::Week, pos)) => {
                //                         if let Some(weeks) = wheel.read().as_ref().weeks() {
                //                             if let Some(slots) = measure(|| weeks.drill_down(pos)) {
                //                                 let mut bars = Vec::new();
                //                                 let mut pos = 0.5;
                //                                 for (i, s) in slots.iter().enumerate() {
                //                                     let bar = Bar::new(pos, *s as f64)
                //                                         .name(fmt_str(i, Granularity::Day));
                //                                     pos += 1.0;
                //                                     bars.push(bar);
                //                                 }
                //                                 let days_chart = BarChart::new(bars)
                //                                     .width(0.7)
                //                                     .color(DAY_COLOR)
                //                                     .highlight(true)
                //                                     .name("Days");
                //                                 Plot::new("Drill down")
                //                                     .legend(Legend::default())
                //                                     .auto_bounds_y()
                //                                     .data_aspect(1.0)
                //                                     .show(ui, |plot_ui| {
                //                                         plot_ui.bar_chart(days_chart);
                //                                     });
                //                             }
                //                         }
                //                     }
                //                     Some((Granularity::Year, pos)) => {
                //                         if let Some(years) = wheel.read().as_ref().years() {
                //                             if let Some(slots) = measure(|| years.drill_down(pos)) {
                //                                 let mut bars = Vec::new();
                //                                 let mut pos = 0.5;
                //                                 for (i, s) in slots.iter().enumerate() {
                //                                     let bar = Bar::new(pos, *s as f64)
                //                                         .name(fmt_str(i, Granularity::Week));
                //                                     pos += 1.0;
                //                                     bars.push(bar);
                //                                 }
                //                                 let weeks_chart = BarChart::new(bars)
                //                                     .width(0.7)
                //                                     .color(WEEK_COLOR)
                //                                     .highlight(true)
                //                                     .name("Weeks");
                //                                 Plot::new("Drill down")
                //                                     .auto_bounds_y()
                //                                     .legend(Legend::default())
                //                                     .data_aspect(1.0)
                //                                     .show(ui, |plot_ui| {
                //                                         plot_ui.bar_chart(weeks_chart);
                //                                     });
                //                             }
                //                         }
                //                     }
                //                     _ => {
                //                         empty_plot(ui);
                //                     }
                // }
                // },
                // );
                // }
                // }
            })
            .response
    }
}

impl eframe::App for TemplateApp {
    /// Called by the frame work to save state before shutdown.
    fn save(&mut self, storage: &mut dyn eframe::Storage) {
        eframe::set_value(storage, eframe::APP_KEY, self);
    }

    /// Called each time the UI needs repainting, which may be many times per second.
    /// Put your widgets into a `SidePanel`, `TopPanel`, `CentralPanel`, `Window` or `Area`.
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        let Self {
            labels,
            tick_granularity,
            log,
            star_wheel,
            timestamp: _,
            aggregate,
            ticks,
            encoded_bytes_len,
            compressed_bytes_len,
            about,
            about_open,
            insert_date,
            insert_time,
            start_date,
            start_time,
            end_date,
            end_time,
            query_result,
            explain_query,
        } = self;

        let update_haw_labels =
            |labels: &mut HawLabels, wheel: &Rc<RefCell<RwWheel<DemoAggregator>>>| {
                let wheel = wheel.borrow();
                labels.watermark_label = to_offset_datetime(wheel.read().watermark()).to_string();
                labels.watermark_unix_label = wheel.read().watermark().to_string();
                labels.remaining_ticks_label = wheel.read().remaining_ticks().to_string();
                labels.slots_len_label = wheel.read().len().to_string();
                labels.landmark_window_label = wheel.read().landmark().unwrap_or(0).to_string();
                labels.seconds_ticks_label = wheel
                    .read()
                    .as_ref()
                    .seconds()
                    .map(|w| w.ticks_remaining().to_string())
                    .unwrap_or_else(|| "None".to_string());
                labels.minutes_ticks_label = wheel
                    .read()
                    .as_ref()
                    .minutes()
                    .map(|w| w.ticks_remaining().to_string())
                    .unwrap_or_else(|| "None".to_string());
                labels.hours_ticks_label = wheel
                    .read()
                    .as_ref()
                    .hours()
                    .map(|w| w.ticks_remaining().to_string())
                    .unwrap_or_else(|| "None".to_string());
                labels.days_ticks_label = wheel
                    .read()
                    .as_ref()
                    .days()
                    .map(|w| w.ticks_remaining().to_string())
                    .unwrap_or_else(|| "None".to_string());
                labels.weeks_ticks_label = wheel
                    .read()
                    .as_ref()
                    .weeks()
                    .map(|w| w.ticks_remaining().to_string())
                    .unwrap_or_else(|| "None".to_string());
                labels.years_ticks_label = wheel
                    .read()
                    .as_ref()
                    .years()
                    .map(|w| w.ticks_remaining().to_string())
                    .unwrap_or_else(|| "None".to_string());
            };

        let plot_wheel = star_wheel.clone();

        // Examples of how to create different panels and windows.
        // Pick whichever suits you.
        // Tip: a good default choice is to just keep the `CentralPanel`.
        // For inspiration and more examples, go to https://emilk.github.io/egui

        #[cfg(not(target_arch = "wasm32"))] // no File->Quit on web pages!
        egui::TopBottomPanel::top("top_panel_quit").show(ctx, |ui| {
            // The top panel is often a good place for a menu bar:
            egui::menu::bar(ui, |ui| {
                ui.menu_button("File", |ui| {
                    if ui.button("Quit").clicked() {
                        // _frame.borrow().close
                    }
                });
            });
        });

        #[cfg(not(target_arch = "wasm32"))]
        egui::TopBottomPanel::top("top_panel").show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.label("Query Latencies: ");
                let (min, max, mean, p99, p99_9, p99_99, count) = QUERY_LATENCY.with(|hist| {
                    let hist = hist.borrow();
                    let min = hist.min();
                    let max = hist.max();
                    let mean = hist.mean();
                    let p99 = hist.value_at_quantile(0.99);
                    let p99_9 = hist.value_at_quantile(0.999);
                    let p99_99 = hist.value_at_quantile(0.9999);
                    (min, max, mean, p99, p99_9, p99_99, hist.len())
                });
                ui.label(RichText::new(format!("min: {: >4}us", min)).strong());
                ui.label(RichText::new(format!("max: {: >4}us", max)).strong());
                ui.label(RichText::new(format!("mean: {:.2}us", mean)).strong());
                ui.label(RichText::new(format!("p99: {: >4}us", p99)).strong());
                ui.label(RichText::new(format!("p99.9: {: >4}us", p99_9)).strong());
                ui.label(RichText::new(format!("p99.99: {: >4}us", p99_99)).strong());
                ui.label(RichText::new(format!("count: {}", count)).strong());
            });
        });

        egui::SidePanel::left("side_panel").show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.heading("💻 µWheel demo");
                egui::widgets::global_dark_light_mode_buttons(ui);
                egui::warn_if_debug_build(ui);
            });

            ui.separator();
            ui.heading("Insert");

            ui.horizontal(|ui| {
                ui.label("Aggregate: ");
                ui.text_edit_singleline(aggregate);
            });

            let insert_date = insert_date.get_or_insert_with(|| chrono::Local::now().date_naive());
            let insert_button = DatePickerButton::new(insert_date).id_source("InsertDatePicker");

            ui.horizontal(|ui| {
                ui.label("Timestamp: ");
                ui.add(insert_button);
                ui.text_edit_singleline(insert_time);
            });

            if ui.button("Insert").clicked() {
                if let Ok(time) = NaiveTime::parse_from_str(insert_time, "%H:%M:%S") {
                    let timestamp_ms = NaiveDateTime::parse_from_str(
                        &format!("{} {}", insert_date, time),
                        "%Y-%m-%d %H:%M:%S",
                    )
                    .unwrap()
                    .and_utc()
                    .timestamp_millis() as u64;
                    let watermark = star_wheel.borrow().watermark();

                    if timestamp_ms < watermark {
                        log.push_front(LogEntry::Red(format!(
                            "Cannot insert timestamp {} before watermark {}",
                            timestamp_ms, watermark
                        )));
                    } else {
                        match aggregate.parse::<u64>() {
                            Ok(value) => {
                                star_wheel
                                    .borrow_mut()
                                    .insert(Entry::new(value, timestamp_ms));
                                log.push_front(LogEntry::Green(format!(
                                    "Inserted {} with timestamp {}",
                                    aggregate, timestamp_ms
                                )));
                            }
                            Err(_) => {
                                log.push_front(LogEntry::Red(format!(
                                    "Cannot parse {} to a u64",
                                    &aggregate,
                                )));
                            }
                        }
                    }
                } else {
                    log.push_front(LogEntry::Red("Cannot parse time".to_string()));
                }

                /*
                match (aggregate.parse::<u64>(), timestamp.parse::<u64>()) {
                    (Ok(aggregate), Ok(timestamp)) => {
                        star_wheel
                            .borrow_mut()
                            .insert(Entry::new(aggregate, timestamp));
                            star_wheel.borrow_mut().insert(Entry::new(aggregate, timestamp));
                            log.push_front(LogEntry::Green(format!(
                                "Inserted {} with timestamp {}",
                                aggregate, timestamp
                            )));
                    }
                    (Ok(_), Err(_)) => {
                        log.push_front(LogEntry::Red(format!(
                            "Cannot parse {} to a u64 timestamp",
                            &timestamp,
                        )));
                    }
                    (Err(_), Ok(_)) => {
                        log.push_front(LogEntry::Red(format!(
                            "Cannot parse {} to a u64 aggregate",
                            &aggregate,
                        )));
                    }
                    _ => {
                        log.push_front(LogEntry::Red(
                            "Both aggregate and timestamp are invalid format, correct format is u64.".to_string()
                        ));
                    }
                }
                */
            }
            ui.separator();
            ui.heading("Tick");
            egui::ComboBox::from_label("Granularity")
                .selected_text(format!("{:?}", tick_granularity))
                .show_ui(ui, |ui| {
                    ui.selectable_value(tick_granularity, Granularity::Second, "Seconds");
                    ui.selectable_value(tick_granularity, Granularity::Minute, "Minutes");
                    ui.selectable_value(tick_granularity, Granularity::Hour, "Hours");
                    ui.selectable_value(tick_granularity, Granularity::Day, "Days");
                });
            ui.add(
                egui::Slider::new(ticks, 1..=1000)
                    //.step_by(1.0)
                    .text("Ticks"),
            );

            if ui.button("Advance").clicked() {
                // egui::trace!(ui, format!("Ticking with ticks {}", ticks));
                if *ticks > 0 {
                    let time = match tick_granularity {
                        Granularity::Second => uwheel::Duration::seconds(*ticks as i64),
                        Granularity::Minute => uwheel::Duration::minutes(*ticks as i64),
                        Granularity::Hour => uwheel::Duration::hours(*ticks as i64),
                        Granularity::Day => uwheel::Duration::days(*ticks as i64),
                        Granularity::Week | Granularity::Year => {
                            panic!("Not supported for now")
                        }
                    };
                    star_wheel.borrow_mut().advance(time);

                    log.push_front(LogEntry::Green(format!(
                        "Advanced time by {} {:?}",
                        ticks, &tick_granularity
                    )));
                    update_haw_labels(labels, star_wheel);
                }
            }

            ui.separator();

            ui.heading("Info");
            let aggregator_name = std::any::type_name::<DemoAggregator>()
                .split("::")
                .last()
                .unwrap()
                .trim_matches('\"');

            ui.label(RichText::new(format!("Aggregator: {}", aggregator_name)).strong());
            let size_bytes = plot_wheel.borrow().size_bytes();
            ui.label(RichText::new(format!("Memory Size Bytes: {}", size_bytes,)).strong());
            ui.label(
                RichText::new(format!(
                    "Total Wheel Slots: {}",
                    Haw::<DemoAggregator>::TOTAL_WHEEL_SLOTS
                ))
                .strong(),
            );
            ui.label(
                RichText::new(format!(
                    "Cycle Length: {}",
                    Haw::<DemoAggregator>::CYCLE_LENGTH
                ))
                .strong(),
            );
            ui.label(RichText::new(format!("Aggregate space: {} years", uwheel::YEARS)).strong());

            ui.separator();

            ui.heading("State");
            ui.horizontal(|ui| {
                ui.label(RichText::new("Watermark (Unix Timestamp): ").strong());
                ui.label(RichText::new(&*labels.watermark_unix_label).strong());
            });
            ui.horizontal(|ui| {
                ui.label(RichText::new("Watermark: ").strong());
                ui.label(RichText::new(&*labels.watermark_label).strong());
            });
            let write_ahead_len = star_wheel.borrow().write().write_ahead_len();
            ui.horizontal(|ui| {
                ui.label(RichText::new("Write ahead Slots: ").strong());
                ui.label(RichText::new(write_ahead_len.to_string()).strong());
            });
            let write_ahead_ms =
                core::time::Duration::from_secs(write_ahead_len as u64).as_millis();
            let max_write_ahead_ts = star_wheel.borrow().read().watermark() + write_ahead_ms as u64;
            ui.horizontal(|ui| {
                ui.label(RichText::new("Max write ahead ts: ").strong());
                ui.label(RichText::new(max_write_ahead_ts.to_string()).strong());
            });
            ui.horizontal(|ui| {
                ui.label(RichText::new("Slots used: ").strong());
                ui.label(RichText::new(&*labels.slots_len_label).strong());
            });
            ui.horizontal(|ui| {
                ui.label(RichText::new("Cycle time: ").strong());
                ui.label(
                    RichText::new(
                        star_wheel
                            .borrow()
                            .read()
                            .current_time_in_cycle()
                            .to_string(),
                    )
                    .strong(),
                );
            });
            ui.horizontal(|ui| {
                ui.label(RichText::new("Remaining ticks: ").strong());
                ui.label(RichText::new(&*labels.remaining_ticks_label).strong());
            });
            ui.horizontal(|ui| {
                ui.label(RichText::new("Seconds ticks remaining: ").strong());
                ui.label(RichText::new(&*labels.seconds_ticks_label).strong());
            });
            ui.horizontal(|ui| {
                ui.label(RichText::new("Minutes ticks remaining: ").strong());
                ui.label(RichText::new(&*labels.minutes_ticks_label).strong());
            });
            ui.horizontal(|ui| {
                ui.label(RichText::new("Hours ticks remaining: ").strong());
                ui.label(RichText::new(&*labels.hours_ticks_label).strong());
            });
            ui.horizontal(|ui| {
                ui.label(RichText::new("Days ticks remaining: ").strong());
                ui.label(RichText::new(&*labels.days_ticks_label).strong());
            });
            ui.horizontal(|ui| {
                ui.label(RichText::new("Weeks ticks remaining: ").strong());
                ui.label(RichText::new(&*labels.weeks_ticks_label).strong());
            });
            ui.horizontal(|ui| {
                ui.label(RichText::new("Years ticks remaining: ").strong());
                ui.label(RichText::new(&*labels.years_ticks_label).strong());
            });

            ui.separator();

            ui.horizontal(|ui| {
                if ui.button("Reset").clicked() {
                    let watermark = star_wheel.borrow().watermark();
                    *star_wheel.borrow_mut() = build_wheel(watermark);
                    update_haw_labels(labels, star_wheel);
                }
                if ui.button("Simulate").clicked() {
                    for _i in 0..1000 {
                        let time = star_wheel.borrow().watermark();
                        for _x in 0..60 {
                            let ts = fastrand::u64(time..time + 60000);
                            let agg = fastrand::u64(1..5);
                            star_wheel.borrow_mut().insert(Entry::new(agg, ts));
                        }
                        star_wheel.borrow_mut().advance(60.seconds());
                        update_haw_labels(labels, star_wheel);
                    }
                }
            });
            ui.separator();

            ui.with_layout(egui::Layout::bottom_up(egui::Align::LEFT), |ui| {
                ui.horizontal(|ui| {
                    ui.spacing_mut().item_spacing.x = 0.0;
                    ui.label("powered by ");
                    ui.hyperlink_to("egui", "https://github.com/emilk/egui");
                    ui.label(" and ");
                    ui.hyperlink_to(
                        "eframe",
                        "https://github.com/emilk/egui/tree/master/crates/eframe",
                    );
                    ui.label(".");
                });
            });
        });

        egui::SidePanel::right("query_panel").show(ctx, |ui| {
            ui.heading("🗠 Query Panel");
            ui.separator();
            ui.heading("Intervals");
            // TODO: add measure on each call
            ui.horizontal(|ui| {
                ui.label(RichText::new("Last 5 seconds: ").strong());
                ui.label(
                    RichText::new(
                        plot_wheel
                            .borrow()
                            .read()
                            .interval(5.seconds())
                            .unwrap_or(0)
                            .to_string(),
                    )
                    .strong(),
                );
            });
            ui.horizontal(|ui| {
                ui.label(RichText::new("Last 15 seconds: ").strong());
                ui.label(
                    RichText::new(
                        plot_wheel
                            .borrow()
                            .read()
                            .interval(15.seconds())
                            .unwrap_or(0)
                            .to_string(),
                    )
                    .strong(),
                );
            });
            ui.horizontal(|ui| {
                ui.label(RichText::new("Last 30 seconds: ").strong());
                ui.label(
                    RichText::new(
                        plot_wheel
                            .borrow()
                            .read()
                            .interval(30.seconds())
                            .unwrap_or(0)
                            .to_string(),
                    )
                    .strong(),
                );
            });
            ui.horizontal(|ui| {
                ui.label(RichText::new("Last minute: ").strong());
                ui.label(
                    RichText::new(
                        plot_wheel
                            .borrow()
                            .read()
                            .interval(1.minutes())
                            .unwrap_or(0)
                            .to_string(),
                    )
                    .strong(),
                );
            });
            ui.horizontal(|ui| {
                ui.label(RichText::new("Last hour: ").strong());
                ui.label(
                    RichText::new(
                        plot_wheel
                            .borrow()
                            .read()
                            .interval(1.hours())
                            .unwrap_or(0)
                            .to_string(),
                    )
                    .strong(),
                );
            });
            // ui.horizontal(|ui| {
            //     ui.label(RichText::new("Last day: ").strong());
            //     ui.label(
            //         RichText::new(
            //             plot_wheel
            //                 .borrow()
            //                 .read()
            //                 .interval(1.days())
            //                 .unwrap_or(0)
            //                 .to_string(),
            //         )
            //         .strong(),
            //     );
            // });
            ui.horizontal(|ui| {
                ui.label(RichText::new("Landmark Window: ").strong());
                let landmark = measure(|| plot_wheel.borrow().read().landmark().unwrap_or(0));
                ui.label(RichText::new(landmark.to_string()).strong());
            });
            ui.separator();

            ui.heading("Custom Range");
            let start_date =
                start_date.get_or_insert_with(|| chrono::offset::Utc::now().date_naive());
            let start_button = DatePickerButton::new(start_date).id_source("StartDatePicker");

            let end_date = end_date.get_or_insert_with(|| chrono::offset::Utc::now().date_naive());
            let end_button = DatePickerButton::new(end_date).id_source("EndDatePicker");

            ui.horizontal(|ui| {
                ui.label("Start Date");
                ui.add(start_button);
                ui.text_edit_singleline(start_time);
            });

            ui.horizontal(|ui| {
                ui.label("End Date");
                ui.add(end_button);
                ui.text_edit_singleline(end_time);
            });

            ui.label(format!("Result: {}", query_result));
            ui.horizontal(|ui| {
                if ui.button("Run").clicked() {
                    let start = NaiveDateTime::parse_from_str(
                        &format!("{} {}", start_date, start_time),
                        "%Y-%m-%d %H:%M:%S",
                    )
                    .unwrap();
                    let end = NaiveDateTime::parse_from_str(
                        &format!("{} {}", end_date, end_time),
                        "%Y-%m-%d %H:%M:%S",
                    )
                    .unwrap();
                    let start_ms = start.and_utc().timestamp_millis() as u64;
                    let end_ms = end.and_utc().timestamp_millis() as u64;

                    // SAFETY: ensure that start_ms is less than end_ms
                    if end_ms > start_ms {
                        let range = WheelRange::new_unchecked(start_ms, end_ms);

                        let res = measure(|| {
                            plot_wheel
                                .borrow()
                                .read()
                                .combine_range_and_lower(range)
                                .unwrap_or(0)
                        });

                        *query_result = res.to_string();

                        if *explain_query {
                            if let Some(plan) = plot_wheel
                                .borrow()
                                .read()
                                .as_ref()
                                .explain_combine_range(range)
                            {
                                log.push_front(LogEntry::Green(format!("Query Plan: {:#?}", plan)));
                            } else {
                                dbg!("No plan found");
                            }
                        }
                    } else {
                        log.push_front(LogEntry::Red(
                            "End range needs to be greater than start range".to_string(),
                        ));
                    }
                }
                ui.checkbox(explain_query, "EXPLAIN ANALYZE");
            });

            ui.separator();

            ui.heading("Serialize µWheel");

            ui.label(
                RichText::new(format!("Encoded bytes (postcard): {}", encoded_bytes_len)).strong(),
            );
            ui.label(
                RichText::new(format!("Compressed bytes (lz4): {}", compressed_bytes_len)).strong(),
            );
            if ui.button("Run").clicked() {
                let wheel = plot_wheel.borrow();
                let bytes = to_allocvec(wheel.read()).unwrap();
                let lz4_compressed = lz4_flex::compress_prepend_size(&bytes);
                *encoded_bytes_len = bytes.len();
                *compressed_bytes_len = lz4_compressed.len();
            }

            ui.separator();

            ui.heading("Log");
            let text_style = egui::TextStyle::Body;
            let row_height = ui.text_style_height(&text_style);
            ScrollArea::vertical().auto_shrink([false; 2]).show_rows(
                ui,
                row_height,
                log.len(),
                |ui, _| {
                    for entry in log {
                        match &entry {
                            LogEntry::Red(entry) => {
                                ui.label(RichText::new(entry).color(Color32::RED).strong())
                            }
                            LogEntry::Green(entry) => {
                                ui.label(RichText::new(entry).color(Color32::GREEN).strong())
                            }
                        };
                    }
                },
            );
        });

        let _open = about_open.get();
        about.show(ctx, &mut true);

        egui::CentralPanel::default().show(ctx, |ui| {
            // The central panel the region left after adding TopPanel's and SidePanel's
            self.wheels_plot(plot_wheel, ui);
        });

        if false {
            egui::Window::new("Window").show(ctx, |ui| {
                ui.label("Windows can be moved by dragging them.");
                ui.label("They are automatically sized based on contents.");
                ui.label("You can turn on resizing and scrolling if you like.");
                ui.label("You would normally choose either panels OR windows.");
            });
        }
    }
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub enum LogEntry {
    Red(String),
    Green(String),
}

// Copied from egui demo and adjusted.
#[derive(Default, serde::Deserialize, serde::Serialize)]
pub struct About {}

impl About {
    fn name(&self) -> &'static str {
        "About µWheel"
    }

    pub fn show(&mut self, ctx: &egui::Context, open: &mut bool) {
        egui::Window::new(self.name())
            .default_width(320.0)
            .default_height(480.0)
            .open(open)
            // .resizable([true, false])
            .show(ctx, |ui| {
                self.ui(ui);
            });
    }
    fn ui(&mut self, ui: &mut egui::Ui) {
        use egui::special_emojis::{OS_APPLE, OS_LINUX, OS_WINDOWS};

        ui.heading("µWheel");

        ui.vertical_centered(|ui| {
            ui.add(
                egui::Image::new(egui::include_image!("../../../assets/logo.png"))
                    .max_width(150.0)
                    .max_height(150.0),
            );
        });
        // ui.add_space(12.0); // ui.separator();

        ui.label("µWheel is an Embeddable Aggregate Management System for Streams and Queries written in Rust.");
        ui.add_space(12.0); // ui.separator();

        ui.label("This demo showcases how µWheel works. The panel on the left side lets you insert data and advance the time of the system. \
            The central panel shows how wheel slots are rolled up over time. The y-axis represents SUM aggregate data that you insert \
            and the x-axis represents low-watermark indexed wheel slots. The panel on the right is for queries and playing around with serialization/compression.".to_string()
        );

        ui.add_space(12.0); // ui.separator();

        ui.label(format!(
            "µWheel is highly embeddable and runs on the web and natively on {}{}{}.",
            OS_APPLE, OS_LINUX, OS_WINDOWS,
        ));

        ui.add_space(12.0); // ui.separator();

        ui.heading("Links");
        links(ui);
        // ui.add_space(12.0);
    }
}

fn links(ui: &mut egui::Ui) {
    use egui::special_emojis::GITHUB;
    ui.hyperlink_to(
        format!("{GITHUB} µWheel on GitHub"),
        "https://github.com/uwheel/uwheel",
    );
    ui.hyperlink_to("µWheel documentation", "https://docs.rs/uwheel/");
}
