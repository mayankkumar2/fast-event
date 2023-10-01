use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone)]
struct ConnectionClosed;

impl Display for ConnectionClosed {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "error: connection closed")
    }
}

impl Error for ConnectionClosed {}

#[derive(PartialEq)]
pub enum FragmentationProcessorError {
    ConnectionClosed,
    UnexpectedError,
}

#[derive(Debug, Clone)]
pub struct OffsetErr;

impl Display for OffsetErr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "error: idx passed is invalid")
    }
}

impl Error for OffsetErr {}
