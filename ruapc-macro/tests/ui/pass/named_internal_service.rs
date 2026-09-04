//! Trait-level options can select a stable wire name and internal visibility.

use std::sync::Arc;

#[ruapc::service(name = "Control", internal)]
pub trait RenamedControlService {
    async fn ping(&self, ctx: &ruapc::Context, req: &()) -> ruapc::Result<()>;
}

struct ControlImpl;

impl RenamedControlService for ControlImpl {
    async fn ping(&self, _ctx: &ruapc::Context, _req: &()) -> ruapc::Result<()> {
        Ok(())
    }
}

fn main() {
    assert_eq!(
        <ControlImpl as RenamedControlService>::NAME,
        "Control"
    );

    let mut router = ruapc::Router::default();
    Arc::new(ControlImpl).ruapc_export(&mut router);

    assert!(!router.is_public_method("Control/ping"));
    assert!(!router.method_names().any(|name| name == "Control/ping"));
}
