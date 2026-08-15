#[derive(Default, Debug)]
pub struct Decoder {
    buffer: Vec<u8>,
    data: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    pub data: String,
}

impl Decoder {
    pub fn push(&mut self, bytes: &[u8]) -> Result<Vec<Event>, String> {
        self.buffer.extend_from_slice(bytes);
        let mut events = Vec::new();
        while let Some(pos) = self.buffer.iter().position(|b| *b == b'\n') {
            let mut line = self.buffer.drain(..=pos).collect::<Vec<_>>();
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            let line = std::str::from_utf8(&line).map_err(|e| format!("SSE is not UTF-8: {e}"))?;
            if line.is_empty() {
                if !self.data.is_empty() {
                    events.push(Event {
                        data: self.data.join("\n"),
                    });
                    self.data.clear();
                }
            } else if !line.starts_with(':') {
                if let Some(v) = line.strip_prefix("data:") {
                    self.data.push(v.strip_prefix(' ').unwrap_or(v).to_owned());
                }
            }
        }
        Ok(events)
    }
    pub fn finish(&mut self) -> Result<Vec<Event>, String> {
        self.push(b"\n\n")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn split_and_multiline() {
        let mut d = Decoder::default();
        assert!(d.push(b"data: hel").unwrap().is_empty());
        let e = d.push(b"lo\r\ndata: world\r\n\r\n").unwrap();
        assert_eq!(e[0].data, "hello\nworld");
    }
}
