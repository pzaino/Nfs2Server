// src/nfs2.rs

use crate::export::Exports;
use crate::rpc::{decode_call, rpc_accept_reply, rpc_prog_mismatch_reply};
use crate::xdr::{XdrR, XdrW};

use std::{
    fs,
    //io::{Read, Seek},
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UdpSocket};
use tracing::{info, warn};

const NFS_PROG: u32 = 100003;
const NFS_VERS: u32 = 2;

// NFSv2 status codes
const NFS_OK: u32 = 0;
const NFSERR_NOENT: u32 = 2;
const NFSERR_ACCES: u32 = 13;

// ------------------------------------------------------------
// File handle helpers
// ------------------------------------------------------------

pub fn fh_from_path(path: &Path) -> Vec<u8> {
    let meta = fs::metadata(path).ok();

    let mut w = XdrW::new();

    let (dev, ino) = if let Some(m) = meta {
        (m.dev(), m.ino())
    } else {
        (0, 0)
    };

    // Very simple, stable handle
    w.put_u32((dev >> 32) as u32);
    w.put_u32(dev as u32);
    w.put_u32((ino >> 32) as u32);
    w.put_u32(ino as u32);

    let mut v = w.buf.to_vec();
    v.resize(32, 0);
    v
}

fn path_from_fh(root: &Path, fh: &[u8]) -> Option<PathBuf> {
    if fh.len() < 16 {
        return None;
    }

    let ino =
        ((fh[8] as u64) << 24) | ((fh[9] as u64) << 16) | ((fh[10] as u64) << 8) | (fh[11] as u64);

    fn walk(base: &Path, target: u64) -> Option<PathBuf> {
        let meta = fs::symlink_metadata(base).ok()?;
        if meta.ino() == target {
            return Some(base.to_path_buf());
        }

        if meta.is_dir() {
            for e in fs::read_dir(base).ok()? {
                let p = e.ok()?.path();
                if let Some(found) = walk(&p, target) {
                    return Some(found);
                }
            }
        }
        None
    }

    walk(root, ino)
}

// ------------------------------------------------------------
// XDR helpers
// ------------------------------------------------------------

fn put_fattr(w: &mut XdrW, meta: &fs::Metadata) {
    let ftype = if meta.is_dir() { 2 } else { 1 };

    let blocks = (meta.len() + 511) / 512;

    w.put_u32(ftype); // type
    w.put_u32(meta.mode()); // mode
    w.put_u32(meta.nlink() as u32); // nlink
    w.put_u32(meta.uid()); // uid
    w.put_u32(meta.gid()); // gid
    w.put_u32(meta.len() as u32); // size
    w.put_u32(4096); // blocksize
    w.put_u32(0); // rdev
    w.put_u32(blocks as u32); // blocks
    w.put_u32(0); // fsid
    w.put_u32(meta.ino() as u32); // fileid

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

// ------------------------------------------------------------

#[derive(Clone)]
pub struct Nfs2 {
    exports: Exports,
}

impl Nfs2 {
    pub fn new(exports: Exports) -> Self {
        Self { exports }
    }

    // --------------------------------------------------------
    // Core RPC handler
    // --------------------------------------------------------

    fn handle_call(&self, buf: &[u8], peer: &str) -> Option<Vec<u8>> {
        let (call, ofs) = decode_call(buf)?;

        // Explicit NFSv3 rejection (THIS FIXES macOS)
        if call.prog == NFS_PROG && call.vers != NFS_VERS {
            info!(
                peer,
                vers = call.vers,
                "nfs2: rejecting unsupported NFS version"
            );
            return Some(rpc_prog_mismatch_reply(call.xid, 2, 2));
        }

        if call.prog != NFS_PROG || call.vers != NFS_VERS {
            return None;
        }

        let mut r = XdrR::new(&buf[ofs..]);
        let root = Path::new("/tmp");

        info!(peer, xid = call.xid, procid = call.procid, "nfs2: request");

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

                if let Some(p) = path_from_fh(root, &fh) {
                    if let Ok(meta) = fs::metadata(&p) {
                        info!(
                            peer,
                            path = %p.display(),
                            size = meta.len(),
                            ino = meta.ino(),
                            mode = format_args!("{:o}", meta.permissions().mode()),
                            "nfs2: GETATTR metadata"
                        );
                        w.put_u32(NFS_OK);
                        put_fattr(&mut w, &meta);
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

                if let Some(dir) = path_from_fh(root, &dirfh) {
                    let p = dir.join(name);
                    if let Ok(meta) = fs::metadata(&p) {
                        w.put_u32(NFS_OK);
                        w.put_opaque(&fh_from_path(&p));
                        put_fattr(&mut w, &meta);
                    } else {
                        w.put_u32(NFSERR_NOENT);
                    }
                } else {
                    w.put_u32(NFSERR_NOENT);
                }

                rpc_accept_reply(call.xid, 0, &w.buf)
            }

            // READDIR
            16 => {
                let fh = r.get_opaque().unwrap_or_default();
                let cookie = r.get_u32().unwrap_or(0);
                let _count = r.get_u32().unwrap_or(0);

                let mut w = XdrW::new();

                if let Some(dir) = path_from_fh(root, &fh) {
                    if let Ok(rd) = fs::read_dir(&dir) {
                        w.put_u32(NFS_OK);

                        let mut idx = 0u32;
                        for e in rd.flatten() {
                            if idx < cookie {
                                idx += 1;
                                continue;
                            }

                            let name = e.file_name().to_string_lossy().into_owned();
                            w.put_u32(1);
                            w.put_u32(e.metadata().map(|m| m.ino() as u32).unwrap_or(0));
                            w.put_string(&name);
                            w.put_u32(idx + 1);
                            idx += 1;
                        }

                        w.put_u32(0);
                    } else {
                        w.put_u32(NFSERR_NOENT);
                    }
                } else {
                    w.put_u32(NFSERR_NOENT);
                }

                rpc_accept_reply(call.xid, 0, &w.buf)
            }

            _ => {
                warn!(peer, procid = call.procid, "nfs2: unimplemented proc");
                let w = XdrW::new();
                rpc_accept_reply(call.xid, 0, &w.buf)
            }
        };

        Some(reply)
    }

    // --------------------------------------------------------
    // UDP server
    // --------------------------------------------------------

    pub async fn run_udp(self, sock: UdpSocket) {
        let mut buf = vec![0u8; 65536];
        info!("nfsd listening (UDP)");

        loop {
            let Ok((n, peer)) = sock.recv_from(&mut buf).await else {
                continue;
            };

            let peer_s = peer.to_string();

            if let Some(reply) = self.handle_call(&buf[..n], &peer_s) {
                let _ = sock.send_to(&reply, peer).await;
            }
        }
    }

    // --------------------------------------------------------
    // TCP server (record-marked)
    // --------------------------------------------------------

    pub async fn run_tcp(self, listener: TcpListener) {
        info!("nfsd listening (TCP)");

        loop {
            let (mut stream, peer) = match listener.accept().await {
                Ok(v) => v,
                Err(_) => continue,
            };

            let this = self.clone();
            let peer_s = peer.to_string();

            info!("nfs2 TCP connected peer={}", peer_s);

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

                    if let Some(reply) = this.handle_call(&buf, &peer_s) {
                        let mut out = Vec::with_capacity(4 + reply.len());
                        out.extend_from_slice(&(0x8000_0000u32 | reply.len() as u32).to_be_bytes());
                        out.extend_from_slice(&reply);

                        if stream.write_all(&out).await.is_err() {
                            break;
                        }
                    }
                }

                info!("nfs2 TCP disconnected peer={}", peer_s);
            });
        }
    }
}
