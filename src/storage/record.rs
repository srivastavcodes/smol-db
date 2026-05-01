use std::io::Write;

use super::page::InternalNode;

use thiserror::Error;

// todo: implement heirarchical encoding/decoding, unless you learn some other idiomatic pattern.

pub type EnDecResult<T> = Result<T, EnDecError>;

#[derive(Error, Debug)]
pub enum EnDecError {
    #[error("I/O error while encoding/decoding")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypeNode {
    Internal = 0,
    Leaf = 1,
}

impl From<TypeNode> for u8 {
    fn from(node: TypeNode) -> Self {
        node as u8
    }
}

impl InternalNode {
    pub fn encode<W: Write>(&self, w: &mut W) -> EnDecResult<()> {
        Ok(())
    }
}
