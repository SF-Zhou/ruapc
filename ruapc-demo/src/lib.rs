#![feature(return_type_notation)]

use ruapc::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Request(pub String);

#[ruapc::service]
pub trait EchoService {
    async fn echo(&self, c: &Context, r: &Request) -> Result<String>;
}

#[ruapc::service]
pub trait GreetService {
    async fn greet(&self, c: &Context, r: &Request) -> Result<String>;
}
