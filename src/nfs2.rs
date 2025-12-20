// src/nfs2.rs

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
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UdpSocket};
use tracing::{info, warn};

// NFS v2
const NFS_PROG: u32 = 100003;
const NFS_VERS: u32 = 2;

// NFS v2 status codes
const NFS_OK: u32 = 0;
const NFSERR_NOENT: u32 = 2;
const NFSERR_ACCES: u32 = 13;

// ---------------------------------------------------------------------------
// File handle helpers
// ---------------------------------------------------------------------------

pub fn fh_from_path(path: &Path) -> Vec<u8> {
    let meta = fs::metadata(path).ok();

    let (dev, ino, mode, uid, gid) = if let Some(m) = meta {
        (m.dev(), m.ino(), m.mode(), m.uid(), m.gid())
    } else {
        (0, 0, 0, 0, 0)
    };

    let mut w = XdrW::new();

    // dev (64)
    w.put_u32((dev >> 32) as u32);
    w.put_u32(dev as u32);

    // ino (64)
    w.put_u32((ino >> 32) as u32);
    w.put_u32(ino as u32);

    w.put_u32(mode);
    w.put_u32(uid);
    w.put_u32(gid);
    w.put_u32(0);

    let mut fh = w.buf.to_vec();
    fh.resize(32, 0);
    fh
}

fn path_from_fh(root: &Path, fh: &[u8]) -> Option<PathBuf> {
    if fh.len() < 32 {
        return None;
    }

    let ino = u32::from_be_bytes([fh[8], fh[9], fh[10], fh[11]]) as u64;

    fn walk(base: &Path, target: u64) -> Option<PathBuf> {
        let meta = fs::symlink_metadata(base).ok()?;
        if meta.ino() == target {
            return Some(base.to_path_buf());
        }
        if meta.is_dir() {
            for e in fs::read_dir(base).ok()? {
                let p = e.ok()?.path();
                if let Some(r) = walk(&p, target) {
                    return Some(r);
                }
            }
        }
        None
    }

    walk(root, ino)
}

// ---------------------------------------------------------------------------
// XDR helpers
// ---------------------------------------------------------------------------

fn xdr_fattr(w: &mut XdrW, meta: &fs::Metadata) {
    let ftype = if meta.is_dir() { 2 } else { 1 }; // NFDIR=2, NFREG=1

    w.put_u32(ftype);
    w.put_u32(meta.mode());
    w.put_u32(meta.nlink() as u32);
    w.put_u32(meta.uid());
    w.put_u32(meta.gid());
    w.put_u32(meta.dev() as u32);
    w.put_u32(meta.ino() as u32);
    w.put_u32(meta.len() as u32);

    let secs = |t: std::time::SystemTime| {
        t.duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as u32)
            .unwrap_or(0)
    };

    w.put_u32(secs(meta.accessed().unwrap_or(std::time::UNIX_EPOCH)));
    w.put_u32(0);
    w.put_u32(secs(meta.modified().unwrap_or(std::time::UNIX_EPOCH)));
    w.put_u32(0);
    w.put_u32(secs(meta.created().unwrap_or(std::time::UNIX_EPOCH)));
    w.put_u32(0);
}

// ---------------------------------------------------------------------------
// NFS server
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct Nfs2 {
    exports: Exports,
}

impl Nfs2 {
    pub fn new(exports: Exports) -> Self {
        Self { exports }
    }

    pub fn handle_call(&self, buf: &[u8]) -> Option<Vec<u8>> {
        let (call, ofs) = decode_call(buf)?;
        if call.prog != NFS_PROG || call.vers != NFS_VERS {
            return None;
        }

        let export_root = self.exports.list().first()?.path.clone();
        let mut r = XdrR::new(&buf[ofs..]);

        let reply = match call.procid {
            0 => {
                let w = XdrW::new();
                rpc_accept_reply(call.xid, NFS_OK, &w.buf)
            }

            // GETATTR
            1 => {
                let fh = r.get_opaque().unwrap_or_default();
                let mut w = XdrW::new();

                if let Some(p) = path_from_fh(&export_root, &fh) {
                    if let Ok(meta) = fs::metadata(&p) {
                        w.put_u32(NFS_OK);
                        xdr_fattr(&mut w, &meta);
                    } else {
                        w.put_u32(NFSERR_NOENT);
                    }
                } else {
                    w.put_u32(NFSERR_NOENT);
                }

                rpc_accept_reply(call.xid, NFS_OK, &w.buf)
            }

            // LOOKUP
            4 => {
                let dirfh = r.get_opaque().unwrap_or_default();
                let name = r.get_string().unwrap_or_default();
                let mut w = XdrW::new();

                if let Some(dir) = path_from_fh(&export_root, &dirfh) {
                    let path = dir.join(&name);
                    if let Ok(meta) = fs::metadata(&path) {
                        w.put_u32(NFS_OK);
                        w.put_opaque(&fh_from_path(&path));
                        xdr_fattr(&mut w, &meta);
                    } else {
                        w.put_u32(NFSERR_NOENT);
                    }
                } else {
                    w.put_u32(NFSERR_NOENT);
                }

                rpc_accept_reply(call.xid, NFS_OK, &w.buf)
            }

            // READ
            6 => {
                let fh = r.get_opaque().unwrap_or_default();
                let offset = r.get_u32().unwrap_or(0) as u64;
                let count = r.get_u32().unwrap_or(0) as usize;
                let _ = r.get_u32();

                let mut w = XdrW::new();

                if let Some(p) = path_from_fh(&export_root, &fh) {
                    if let Ok(meta) = fs::metadata(&p) {
                        if let Ok(mut f) = fs::File::open(&p) {
                            let _ = f.seek(std::io::SeekFrom::Start(offset));
                            let mut buf = vec![0; count.min(8192)];
                            let n = f.read(&mut buf).unwrap_or(0);

                            w.put_u32(NFS_OK);
                            xdr_fattr(&mut w, &meta);
                            w.put_u32(n as u32);
                            w.put_opaque(&buf[..n]);
                        } else {
                            w.put_u32(NFSERR_ACCES);
                        }
                    } else {
                        w.put_u32(NFSERR_NOENT);
                    }
                } else {
                    w.put_u32(NFSERR_NOENT);
                }

                rpc_accept_reply(call.xid, NFS_OK, &w.buf)
            }

            // READDIR
            16 => {
                let fh = r.get_opaque().unwrap_or_default();
                let cookie = r.get_u32().unwrap_or(0);
                let _count = r.get_u32().unwrap_or(0);

                let mut w = XdrW::new();

                if let Some(dir) = path_from_fh(&export_root, &fh) {
                    if let Ok(rd) = fs::read_dir(&dir) {
                        w.put_u32(NFS_OK);

                        let mut idx = 0u32;
                        for e in rd.flatten() {
                            if idx < cookie {
                                idx += 1;
                                continue;
                            }

                            let name = e.file_name().to_string_lossy().into_owned();
                            let ino = e.metadata().map(|m| m.ino() as u32).unwrap_or(0);

                            w.put_u32(1); // entry present
                            w.put_u32(ino);
                            w.put_string(&name);
                            w.put_u32(idx + 1); // cookie
                            idx += 1;
                        }

                        w.put_u32(0); // no more entries
                        w.put_u32(1); // eof = TRUE
                    } else {
                        w.put_u32(NFSERR_NOENT);
                    }
                } else {
                    w.put_u32(NFSERR_NOENT);
                }

                rpc_accept_reply(call.xid, NFS_OK, &w.buf)
            }

            // STATFS
            17 => {
                let mut w = XdrW::new();
                w.put_u32(NFS_OK);
                w.put_u32(4096);
                w.put_u32(4096);
                w.put_u32(1024);
                w.put_u32(512);
                w.put_u32(512);
                rpc_accept_reply(call.xid, NFS_OK, &w.buf)
            }

            p => {
                warn!(procid = p, "nfs2: unsupported proc");
                let w = XdrW::new();
                rpc_accept_reply(call.xid, NFS_OK, &w.buf)
            }
        };

        Some(reply)
    }

    // Runs the NFS server over UDP
    pub async fn run_udp(self, sock: UdpSocket) {
        info!("nfsd listening (UDP)");
        let mut buf = vec![0u8; 32768];
        loop {
            let Ok((n, peer)) = sock.recv_from(&mut buf).await else {
                continue;
            };
            if let Some(r) = self.handle_call(&buf[..n]) {
                let _ = sock.send_to(&r, peer).await;
            }
        }
    }

    // Runs the NFS server over TCP
    pub async fn run_tcp(self, listener: TcpListener) {
        info!("nfsd listening (TCP)");
        loop {
            let (mut s, _) = listener.accept().await.unwrap();
            let this = self.clone();
            tokio::spawn(async move {
                loop {
                    let mut h = [0u8; 4];
                    if s.read_exact(&mut h).await.is_err() {
                        break;
                    }
                    let len = u32::from_be_bytes(h) & 0x7fff_ffff;
                    let mut b = vec![0u8; len as usize];
                    if s.read_exact(&mut b).await.is_err() {
                        break;
                    }
                    if let Some(r) = this.handle_call(&b) {
                        let mut o = Vec::new();
                        o.extend_from_slice(&(0x8000_0000 | r.len() as u32).to_be_bytes());
                        o.extend_from_slice(&r);
                        let _ = s.write_all(&o).await;
                    }
                }
            });
        }
    }
}
