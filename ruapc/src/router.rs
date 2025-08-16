use std::{collections::HashMap, sync::Arc};

use foldhash::fast::RandomState;
use schemars::{JsonSchema, Schema};
use serde::{Deserialize, Serialize};

use crate::{
    Context, Message,
    error::{Error, ErrorKind, Result},
    services::MetaService,
};

pub type Func = Box<dyn Fn(Context, Message) -> Result<()> + Send + Sync>;

#[derive(Debug, Serialize, Deserialize, JsonSchema, Clone)]
pub struct MethodInfo {
    pub req_schema: Schema,
    pub rsp_schema: Schema,
}

pub struct Method {
    pub info: MethodInfo,
    func: Func,
}

impl Method {
    #[must_use]
    pub fn new(req_schema: Schema, rsp_schema: Schema, func: Func) -> Self {
        Self {
            info: MethodInfo {
                req_schema,
                rsp_schema,
            },
            func,
        }
    }
}

pub struct Router {
    pub methods: HashMap<String, Method, RandomState>,
}

impl Default for Router {
    fn default() -> Self {
        Self {
            methods: Arc::new(()).ruapc_export().into_iter().collect(),
        }
    }
}

impl Router {
    pub fn add_methods(&mut self, methods: HashMap<String, Method>) {
        self.methods.extend(methods);
    }

    pub fn method_names(&self) -> impl Iterator<Item = &String> {
        self.methods.keys()
    }

    pub fn dispatch(&self, mut ctx: Context, msg: Message) {
        if let Some(method) = self.methods.get(&msg.meta.method) {
            let _ = (method.func)(ctx, msg);
        } else {
            tokio::spawn(async move {
                let m = format!("method not found: {}", msg.meta.method);
                tracing::error!("{}", m);
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
