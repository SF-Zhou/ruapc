//! Attachment builders cannot bypass their consuming recovery scope.

#[test]
fn buffer_calls_require_a_scope_and_scoped_handles_cannot_escape() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/*.rs");
}
