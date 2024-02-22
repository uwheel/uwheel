fn main() {
    cxx_build::bridge("src/lib.rs")
        .include("mimalloc/include")
        .flag("-L mimalloc/out/release")
        .flag("-lmimalloc")
        .flag("-O3")
        .file("src/FiBA.cc")
        .flag_if_supported("-std=c++17")
        .compile("fiba_rs");
    println!("cargo:rerun-if-changed=src/lib.rs");
    println!("cargo:rerun-if-changed=src/FiBA.cc");
    println!("cargo:rerun-if-changed=include/FiBA.h");
}
