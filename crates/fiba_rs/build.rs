fn main() {
    cxx_build::bridge("src/lib.rs")
        .file("src/FiBA.cc")
        .flag_if_supported("-std=c++14")
        .compile("fiba_rs");
}
