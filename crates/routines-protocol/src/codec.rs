//! Length-prefixed JSON codec for UDS communication.
//!
//! Wire format: `[4 bytes big-endian length][JSON payload]`

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Maximum message size: 64 MiB (stdout can be large).
const MAX_MESSAGE_SIZE: u32 = 64 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Message too large: {size} bytes (max {MAX_MESSAGE_SIZE})")]
    MessageTooLarge { size: u32 },

    #[error("Connection closed")]
    ConnectionClosed,
}

/// Write a length-prefixed JSON message to an async writer.
pub async fn write_message<W, T>(writer: &mut W, msg: &T) -> Result<(), CodecError>
where
    W: AsyncWriteExt + Unpin,
    T: Serialize,
{
    let payload = serde_json::to_vec(msg)?;
    let len = payload.len() as u32;
    if len > MAX_MESSAGE_SIZE {
        return Err(CodecError::MessageTooLarge { size: len });
    }
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(&payload).await?;
    writer.flush().await?;
    Ok(())
}

/// Read a length-prefixed JSON message from an async reader.
pub async fn read_message<R, T>(reader: &mut R) -> Result<T, CodecError>
where
    R: AsyncReadExt + Unpin,
    T: for<'de> Deserialize<'de>,
{
    let mut len_buf = [0u8; 4];
    match reader.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            return Err(CodecError::ConnectionClosed);
        }
        Err(e) => return Err(CodecError::Io(e)),
    }
    let len = u32::from_be_bytes(len_buf);
    if len > MAX_MESSAGE_SIZE {
        return Err(CodecError::MessageTooLarge { size: len });
    }
    let mut buf = vec![0u8; len as usize];
    reader.read_exact(&mut buf).await?;
    let msg = serde_json::from_slice(&buf)?;
    Ok(msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn roundtrip() {
        use crate::types::{Request, Response};

        let req = Request::Ping;
        let mut buf = Vec::new();
        write_message(&mut buf, &req).await.unwrap();

        let mut cursor = std::io::Cursor::new(buf);
        let decoded: Request = read_message(&mut cursor).await.unwrap();
        assert!(matches!(decoded, Request::Ping));

        let resp = Response::Pong;
        let mut buf = Vec::new();
        write_message(&mut buf, &resp).await.unwrap();

        let mut cursor = std::io::Cursor::new(buf);
        let decoded: Response = read_message(&mut cursor).await.unwrap();
        assert!(matches!(decoded, Response::Pong));
    }

    #[tokio::test]
    async fn submit_roundtrip() {
        use crate::types::Request;
        use std::collections::HashMap;

        let mut inputs = HashMap::new();
        inputs.insert("env".to_string(), "prod".to_string());
        let req = Request::Submit {
            routine: "core:deploy".to_string(),
            inputs,
        };

        let mut buf = Vec::new();
        write_message(&mut buf, &req).await.unwrap();

        let mut cursor = std::io::Cursor::new(buf);
        let decoded: Request = read_message(&mut cursor).await.unwrap();
        match decoded {
            Request::Submit { routine, inputs } => {
                assert_eq!(routine, "core:deploy");
                assert_eq!(inputs["env"], "prod");
            }
            _ => panic!("wrong variant"),
        }
    }
}
