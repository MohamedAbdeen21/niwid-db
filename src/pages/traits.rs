pub(crate) trait Serialize {
    fn to_bytes(&self) -> &[u8];
    fn from_bytes(bytes: &[u8]) -> Self;
}
