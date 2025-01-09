use bytes::Bytes;

#[derive(Clone, Debug)]
pub enum StreamMessage {
    Data(Bytes),
    ExitFlag(ExitReason),
}

#[derive(Clone, Debug, PartialEq)]
pub enum ExitReason {
    EndOfStream,
    Error,
    CancelledByExternal,
}
