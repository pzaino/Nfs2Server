// src/xdr.rs

use bytes::{Buf, BufMut, BytesMut};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum XdrError {
    #[error("buffer underrun")]
    Underrun,
    #[error("string too long")]
    StrTooLong,
}

pub struct XdrW {
    pub buf: BytesMut,
}
impl XdrW {
    pub fn new() -> Self {
        Self {
            buf: BytesMut::new(),
        }
    }

    pub fn put_u32(&mut self, v: u32) {
        self.buf.put_u32(v);
    }
    pub fn put_i32(&mut self, v: i32) {
        self.buf.put_i32(v as i32);
    }
    pub fn put_opaque(&mut self, data: &[u8]) {
        self.buf.put_u32(data.len() as u32);
        self.buf.extend_from_slice(data);
        let pad = (4 - (data.len() % 4)) % 4;
        if pad > 0 {
            self.buf.extend_from_slice(&[0; 3][..pad]);
        }
    }
    pub fn put_string(&mut self, s: &str) {
        self.put_opaque(s.as_bytes());
    }
}

pub struct XdrR<'a> {
    pub buf: &'a [u8],
    pub pos: usize,
}
impl<'a> XdrR<'a> {
    pub fn new(b: &'a [u8]) -> Self {
        Self { buf: b, pos: 0 }
    }
}

impl<'a> XdrR<'a> {
    pub fn skip_bytes(&mut self, len: usize) -> Result<(), XdrError> {
        let pad = (4 - (len % 4)) % 4;
        self.need(len + pad)?;
        self.pos += len + pad;
        Ok(())
    }
}

/*
pub trait XdrCodec {
    fn put_u32(&mut self, v: u32);
    fn put_i32(&mut self, v: i32);
    fn put_opaque(&mut self, data: &[u8]);
    fn put_string(&mut self, s: &str);
}

impl XdrCodec for XdrW {
    fn put_u32(&mut self, v: u32) {
        self.buf.put_u32(v);
    }
    fn put_i32(&mut self, v: i32) {
        self.buf.put_i32(v as i32);
    }
    fn put_opaque(&mut self, data: &[u8]) {
        self.buf.put_u32(data.len() as u32);
        self.buf.extend_from_slice(data);
        let pad = (4 - (data.len() % 4)) % 4;
        if pad > 0 {
            self.buf.extend_from_slice(&[0; 3][..pad]);
        }
    }
    fn put_string(&mut self, s: &str) {
        self.put_opaque(s.as_bytes());
    }
}

*/

impl<'a> XdrR<'a> {
    fn need(&self, n: usize) -> Result<(), XdrError> {
        if self.pos + n <= self.buf.len() {
            Ok(())
        } else {
            Err(XdrError::Underrun)
        }
    }
    pub fn get_u32(&mut self) -> Result<u32, XdrError> {
        self.need(4)?;
        let v = u32::from_be_bytes(self.buf[self.pos..self.pos + 4].try_into().unwrap());
        self.pos += 4;
        Ok(v)
    }
    pub fn get_i32(&mut self) -> Result<i32, XdrError> {
        Ok(self.get_u32()? as i32)
    }
    pub fn get_opaque(&mut self) -> Result<Vec<u8>, XdrError> {
        let len = self.get_u32()? as usize;
        let pad = (4 - (len % 4)) % 4;
        self.need(len + pad)?;

        let data = self.buf[self.pos..self.pos + len].to_vec();
        self.pos += len + pad;

        Ok(data)
    }
    pub fn get_string(&mut self) -> Result<String, XdrError> {
        let v = self.get_opaque()?;
        Ok(String::from_utf8_lossy(&v).into())
    }
}
