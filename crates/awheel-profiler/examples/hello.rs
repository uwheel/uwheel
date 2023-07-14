use std::time::Duration;

fn main() {
    awheel_profiler::set_scopes_on(true);
    my_func();
}

fn my_func() {
    awheel_profiler::profile_function!("my_func");
    // Do stuff
    std::thread::sleep(Duration::from_secs(5));
}
