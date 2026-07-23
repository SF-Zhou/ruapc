//! UI tests for the `#[service]` macro: valid definitions must compile
//! end-to-end (including the generated `Client` impls), invalid ones must
//! fail with the macro's diagnostics pinned in `.stderr` files.
//!
//! Regenerate the expected output after intentional changes with
//! `TRYBUILD=overwrite cargo test -p ruapc-macro`.

#[test]
fn trybuild() {
    let t = trybuild::TestCases::new();
    t.pass("tests/ui/pass/*.rs");
    t.compile_fail("tests/ui/fail/*.rs");
}
