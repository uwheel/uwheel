#![warn(clippy::all, rust_2018_idioms)]
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")] // hide console window on Windows in release

mod app;

// When compiling natively:
#[cfg(not(target_arch = "wasm32"))]
fn main() -> eframe::Result<()> {
    // tell puffin to collect data

    use eframe::IconData;
    puffin::set_scopes_on(true);
    // Log to stdout (if you run with `RUST_LOG=debug`).
    tracing_subscriber::fmt::init();

    let mut native_options = eframe::NativeOptions::default();
    let icon_bytes = include_bytes!("../../../assets/logo.png");
    native_options.icon_data = Some(IconData::try_from_png_bytes(icon_bytes).unwrap());
    eframe::run_native(
        "awheel",
        native_options,
        Box::new(|cc| Box::new(app::TemplateApp::new(cc))),
    )
}

/*
// when compiling to web using trunk.
#[cfg(target_arch = "wasm32")]
fn main() {
    // Make sure panics are logged using `console.error`.
    console_error_panic_hook::set_once();

    // Redirect tracing to console.log and friends:
    tracing_wasm::set_as_global_default();

    let web_options = eframe::WebOptions::default();

    wasm_bindgen_futures::spawn_local(async {
        eframe::WebRunner::new()
            .start(
                "the_canvas_id", // hardcode it
                web_options,
                Box::new(|cc| Box::new(app::TemplateApp::new(cc))),
            )
            .await
            .expect("failed to start eframe");
    });
}
*/
