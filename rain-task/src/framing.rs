use std::os::unix::net::UnixStream;
use byteorder::{ReadBytesExt, WriteBytesExt, LittleEndian};
use super::{Result, WorkerToSubworkerMessage, SubworkerToWorkerMessage, MAX_MSG_SIZE, MSG_PROTOCOL};
use std::io::{Read, Write};
use serde_cbor;

/// Auxiliary trait for reading from and writing to sockets.
pub(crate) trait SocketExt {
    fn write_frame(&mut self, &[u8]) -> Result<()>;
    fn read_frame(&mut self) -> Result<Vec<u8>>; 
    fn write_msg(&mut self, &SubworkerToWorkerMessage) -> Result<()>;
    fn read_msg(&mut self) -> Result<WorkerToSubworkerMessage>; 
}

impl SocketExt for UnixStream {
    fn write_msg(&mut self, m: &SubworkerToWorkerMessage) -> Result<()> {
        let data = serde_cbor::to_vec(m)?;
        self.write_frame(&data)
    }

    fn read_msg(&mut self) -> Result<WorkerToSubworkerMessage> {
        let data = self.read_frame()?;
        let msg = serde_cbor::from_slice::<WorkerToSubworkerMessage>(&data)?;
        Ok(msg)
    }

    fn write_frame(&mut self, data: &[u8]) -> Result<()> {
        if data.len() > MAX_MSG_SIZE {
            bail!("write_frame: message too long ({} bytes of {} allowed)", data.len(), MAX_MSG_SIZE);
        }
        self.write_u32::<LittleEndian>(data.len() as u32)?;
        self.write_all(data)?;
        Ok(())
    }

    fn read_frame(&mut self) -> Result<Vec<u8>> {
        let len = self.read_u32::<LittleEndian>()? as usize;
        if len > MAX_MSG_SIZE {
            bail!("read_frame: message too long ({} bytes of {} allowed)", len, MAX_MSG_SIZE);
        }
        let mut data = vec![0; len];
        self.read_exact(&mut data)?;
        Ok(data)
    }
}
