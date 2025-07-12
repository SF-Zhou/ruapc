use foldhash::fast::RandomState;
use std::collections::HashMap;

use crate::{Context, RecvMsg, error::{Result, Error, ErrorKind}};

pub type Method = Box<dyn Fn(Context, RecvMsg) -> Result<()> + Send + Sync>;

#[derive(Default)]
pub struct Router {
    methods: HashMap<String, Method, RandomState>,
}

impl Router {
    pub fn add_methods(&mut self, methods: HashMap<String, Method>) {
        self.methods.extend(methods);
    }

    pub fn method_names(&self) -> impl Iterator<Item = &String> {
        self.methods.keys()
    }

    pub fn dispatch(&self, mut ctx: Context, msg: RecvMsg) {
        if let Some(func) = self.methods.get(&msg.meta.method) {
            let _ = func(ctx, msg);
        } else {
            tokio::spawn(async move {
                let m = format!("method not found: {}", msg.meta.method);
                tracing::error!(m);
                ctx.send_err_rsp(msg.meta, Error::new(ErrorKind::InvalidArgument, m))
                    .await;
            });
        }
    }
}

impl std::fmt::Debug for Router {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Router")
            .field("methods", &self.methods.keys())
            .finish()
    }
}
