// src/nfs2.rs
//
// Minimal, strict NFSv2 server
// Focused on correctness for RISC OS and macOS
//

use crate::{
    export::Exports,
    rpc::{decode_call, rpc_accept_reply},
    xdr::{XdrR, XdrW},
};

use std::{
    fs,
    io::{Read, Seek},
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, UdpSocket},
};

use tracing::{info, warn};

// ---------------------------
// NFS constants (v2)
// ---------------------------

const NFS_PROG: u32 = 100003;
const NFS_VERS: u32 = 2;

// File types
const NFREG: u32 = 1;
const NFDIR: u32 = 2;

// Status
const NFS_OK: u32 = 0;
const NFSERR_NOENT: u32 = 2;
const NFSERR_ACCES: u32 = 13;

// ---------------------------

#[derive(Clone)]
pub struct Nfs2 {
    exports: Exports,
}

impl Nfs2 {
    pub fn new(exports: Exports) -> Self {
        Self { exports }
    }
}

// ---------------------------
// File handle helpers
// ---------------------------

pub fn fh_from_path(path: &Path) -> Vec<u8> {
    let meta = fs::metadata(path).ok();

    let mut w = XdrW::new();

    let (dev, ino) = if let Some(m) = meta {
        (m.dev(), m.ino())
    } else {
        (0, 0)
    };

    // 32-byte opaque handle (traditional)
    w.put_u32((dev >> 32) as u32);
    w.put_u32(dev as u32);
    w.put_u32((ino >> 32) as u32);
    w.put_u32(ino as u32);
    w.put_u32(0);
    w.put_u32(0);
    w.put_u32(0);
    w.put_u32(0);

    let mut fh = w.buf.to_vec();
    fh.resize(32, 0);
    fh
}

fn path_from_fh(root: &Path, fh: &[u8]) -> Option<PathBuf> {
    if fh.len() < 16 {
        return None;
    }

    let ino = u64::from(fh[12])
        | (u64::from(fh[11]) << 8)
        | (u64::from(fh[10]) << 16)
        | (u64::from(fh[9]) << 24);

    fn walk(dir: &Path, target: u64) -> Option<PathBuf> {
        let meta = fs::symlink_metadata(dir).ok()?;
        if meta.ino() == target {
            return Some(dir.to_path_buf());
        }

        if meta.is_dir() {
            for entry in fs::read_dir(dir).ok()? {
                let p = entry.ok()?.path();
                if let Some(found) = walk(&p, target) {
                    return Some(found);
                }
            }
        }
        None
    }

    walk(root, ino)
}

// ---------------------------
// XDR fattr (STRICT NFSv2)
// ---------------------------

fn write_fattr(w: &mut XdrW, meta: &fs::Metadata) {
    let ftype = if meta.is_dir() { NFDIR } else { NFREG };

    w.put_u32(ftype); // type
    w.put_u32(meta.mode()); // mode
    w.put_u32(meta.nlink() as u32); // nlink
    w.put_u32(meta.uid()); // uid
    w.put_u32(meta.gid()); // gid
    w.put_u32(meta.len() as u32); // size
    w.put_u32(4096); // blocksize
    w.put_u32(meta.blocks() as u32); // blocks
    w.put_u32(meta.dev() as u32); // rdev

    // atime
    w.put_u32(meta.atime() as u32);
    w.put_u32(0);

    // mtime
    w.put_u32(meta.mtime() as u32);
    w.put_u32(0);

    // ctime
    w.put_u32(meta.ctime() as u32);
    w.put_u32(0);
}

// ---------------------------
// Core request handler
// ---------------------------

impl Nfs2 {
    fn handle_call(&self, buf: &[u8], peer: &str) -> Option<Vec<u8>> {
        let (call, ofs) = decode_call(buf)?;

        if call.prog != NFS_PROG || call.vers != NFS_VERS {
            warn!(
                peer,
                prog = call.prog,
                vers = call.vers,
                "nfs2: rejecting unsupported prog/version"
            );
            return None;
        }

        let mut r = XdrR::new(&buf[ofs..]);

        info!(peer, xid = call.xid, procid = call.procid, "nfs2: request");

        let export_root = &self.exports.list()[0].path;

        let reply = match call.procid {
            // NULL
            0 => {
                let w = XdrW::new();
                rpc_accept_reply(call.xid, 0, &w.buf)
            }

            // GETATTR
            1 => {
                let fh = r.get_opaque().unwrap_or_default();
                let mut w = XdrW::new();

                if let Some(path) = path_from_fh(export_root, &fh) {
                    if let Ok(meta) = fs::metadata(&path) {
                        w.put_u32(NFS_OK);
                        write_fattr(&mut w, &meta);
                    } else {
                        w.put_u32(NFSERR_NOENT);
                    }
                } else {
                    w.put_u32(NFSERR_NOENT);
                }

                rpc_accept_reply(call.xid, 0, &w.buf)
            }

            // LOOKUP
            4 => {
                let dirfh = r.get_opaque().unwrap_or_default();
                let name = r.get_string().unwrap_or_default();
                let mut w = XdrW::new();

                if let Some(dir) = path_from_fh(export_root, &dirfh) {
                    let path = dir.join(&name);
                    if let Ok(meta) = fs::metadata(&path) {
                        w.put_u32(NFS_OK);
                        w.put_opaque(&fh_from_path(&path));
                        write_fattr(&mut w, &meta);
                    } else {
                        w.put_u32(NFSERR_NOENT);
                    }
                } else {
                    w.put_u32(NFSERR_NOENT);
                }

                rpc_accept_reply(call.xid, 0, &w.buf)
            }

            // READ
            6 => {
                let fh = r.get_opaque().unwrap_or_default();
                let offset = r.get_u32().unwrap_or(0) as u64;
                let count = r.get_u32().unwrap_or(0) as usize;
                let _ = r.get_u32(); // totalcount

                let mut w = XdrW::new();

                if let Some(path) = path_from_fh(export_root, &fh) {
                    if let Ok(mut f) = fs::File::open(&path) {
                        let _ = f.seek(std::io::SeekFrom::Start(offset));
                        let mut buf = vec![0u8; count.min(8192)];
                        let n = f.read(&mut buf).unwrap_or(0);

                        w.put_u32(NFS_OK);
                        w.put_u32((offset + n as u64) as u32);
                        w.put_opaque(&buf[..n]);
                    } else {
                        w.put_u32(NFSERR_NOENT);
                    }
                } else {
                    w.put_u32(NFSERR_NOENT);
                }

                rpc_accept_reply(call.xid, 0, &w.buf)
            }

            _ => {
                warn!(peer, procid = call.procid, "nfs2: unsupported proc");
                let w = XdrW::new();
                rpc_accept_reply(call.xid, 0, &w.buf)
            }
        };

        Some(reply)
    }

    // ---------------------------
    // UDP server
    // ---------------------------

    pub async fn run_udp(self, sock: UdpSocket) {
        let mut buf = vec![0u8; 32768];

        loop {
            let Ok((n, peer)) = sock.recv_from(&mut buf).await else {
                continue;
            };

            let peer_str = peer.to_string();

            if let Some(reply) = self.handle_call(&buf[..n], &peer_str) {
                let _ = sock.send_to(&reply, peer).await;
            }
        }
    }

    // ---------------------------
    // TCP server (record-marked)
    // ---------------------------

    pub async fn run_tcp(self, listener: TcpListener) {
        loop {
            let (mut stream, peer) = match listener.accept().await {
                Ok(v) => v,
                Err(_) => continue,
            };

            let this = self.clone();
            let peer_str = peer.to_string();

            info!(peer = %peer_str, "nfs2 TCP connected");

            tokio::spawn(async move {
                loop {
                    let mut hdr = [0u8; 4];
                    if stream.read_exact(&mut hdr).await.is_err() {
                        break;
                    }

                    let marker = u32::from_be_bytes(hdr);
                    let len = (marker & 0x7fff_ffff) as usize;

                    let mut buf = vec![0u8; len];
                    if stream.read_exact(&mut buf).await.is_err() {
                        break;
                    }

                    if let Some(reply) = this.handle_call(&buf, &peer_str) {
                        let mut out = Vec::with_capacity(4 + reply.len());
                        out.extend_from_slice(&(0x8000_0000u32 | reply.len() as u32).to_be_bytes());
                        out.extend_from_slice(&reply);

                        if stream.write_all(&out).await.is_err() {
                            break;
                        }
                    }
                }

                info!(peer = %peer_str, "nfs2 TCP disconnected");
            });
        }
    }
}
