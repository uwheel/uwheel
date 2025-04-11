#![warn(clippy::all, rust_2018_idioms)]
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")] // hide console window on Windows in release

mod app;

// When compiling natively:
#[cfg(not(target_arch = "wasm32"))]
fn main() -> eframe::Result<()> {
    // Log to stdout (if you run with `RUST_LOG=debug`).
    tracing_subscriber::fmt::init();

    let native_options = eframe::NativeOptions {
        viewport: eframe::egui::ViewportBuilder::default().with_icon(
            // NOTE: Adding an icon is optional
            eframe::icon_data::from_png_bytes(&include_bytes!("../../../assets/logo.png")[..])
                .expect("Failed to load icon"),
        ),
        ..Default::default()
    };

    eframe::run_native(
        "µWheel",
        native_options,
        Box::new(|cc| {
            egui_extras::install_image_loaders(&cc.egui_ctx);
            Box::new(app::TemplateApp::new(cc))
        }),
    )
}

#[cfg(target_arch = "wasm32")]
fn main() {
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
