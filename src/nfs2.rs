// src/nfs2.rs

use crate::{
    export::Exports,
    rpc::{decode_call, rpc_accept_reply},
    xdr::{XdrR, XdrW},
};
use std::{
    fs, io,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UdpSocket};
use tracing::{info, warn};

// NFS v2 program 100003, version 2
const NFS_PROG: u32 = 100003;
const NFS_VERS: u32 = 2;

// Implement minimal: NULL(0), GETATTR(1), LOOKUP(4), READ(6), READDIR(16), STATFS(17)

#[derive(Clone)]
pub struct Nfs2 {
    exports: Exports,
}

impl Nfs2 {
    pub fn new(exports: Exports) -> Self {
        Self { exports }
    }
}

pub fn fh_from_path(path: &str) -> Vec<u8> {
    // 32 bytes file handle: [dev(8)|ino(8)|mode(4)|uid(4)|gid(4)|pad(4)]
    let meta = fs::metadata(path).ok();
    let mut w = XdrW::new();
    let (dev, ino, mode, uid, gid) = if let Some(m) = meta {
        (m.dev() as u64, m.ino() as u64, m.mode(), m.uid(), m.gid())
    } else {
        (0, 0, 0, 0, 0)
    };

    w.put_u32((dev >> 32) as u32);
    w.put_u32(dev as u32);
    w.put_u32((ino >> 32) as u32);
    w.put_u32(ino as u32);
    w.put_u32(mode);
    w.put_u32(uid);
    w.put_u32(gid);
    w.put_u32(0);

    let mut v = w.buf.to_vec();
    v.resize(32, 0);
    v
}

fn path_from_fh(root: &Path, fh: &[u8]) -> Option<PathBuf> {
    if fh.len() < 32 {
        return None;
    }

    // low 32 bits of ino as encoded by fh_from_path()
    let ino =
        ((fh[8] as u64) << 24) | ((fh[9] as u64) << 16) | ((fh[10] as u64) << 8) | fh[11] as u64;

    fn find_by_ino(base: &Path, target: u64) -> Option<PathBuf> {
        let meta = fs::symlink_metadata(base).ok()?;
        if meta.ino() as u64 == target {
            return Some(base.to_path_buf());
        }
        if meta.is_dir() {
            for e in fs::read_dir(base).ok()? {
                let p = e.ok()?.path();
                if let Some(m) = find_by_ino(&p, target) {
                    return Some(m);
                }
            }
        }
        None
    }

    find_by_ino(root, ino)
}

fn xdr_fattr(w: &mut XdrW, meta: &fs::Metadata) {
    let ftype = if meta.is_dir() { 2 } else { 1 }; // NFREG=1, NFDIR=2
    w.put_u32(ftype);
    w.put_u32(meta.mode());
    w.put_u32(meta.nlink() as u32);
    w.put_u32(meta.uid());
    w.put_u32(meta.gid());
    w.put_u32(meta.dev() as u32);
    w.put_u32(meta.ino() as u32);
    w.put_u32(meta.len() as u32);

    let secs = |t: std::time::SystemTime| -> u32 {
        t.duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as u32)
            .unwrap_or(0)
    };

    w.put_u32(secs(
        meta.accessed()
            .ok()
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH),
    ));
    w.put_u32(0);
    w.put_u32(secs(
        meta.modified()
            .ok()
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH),
    ));
    w.put_u32(0);
    w.put_u32(secs(
        meta.created()
            .ok()
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH),
    ));
    w.put_u32(0);
}

impl Nfs2 {
    /// Core NFS handler (UDP + TCP). Transport independent.
    pub fn handle_call(&self, buf: &[u8]) -> Option<Vec<u8>> {
        let (call, ofs) = decode_call(buf)?;
        if call.prog != NFS_PROG || call.vers != NFS_VERS {
            return None;
        }

        // NOTE: unchanged semantics (still hardcoded for now)
        let export_root = self.exports.list().first()?.path.clone();

        let mut r = XdrR::new(&buf[ofs..]);

        let reply = match call.procid {
            0 => {
                // NULL
                let w = XdrW::new();
                rpc_accept_reply(call.xid, 0, &w.buf)
            }

            1 => {
                // GETATTR
                let fh = r.get_opaque().unwrap_or_default();
                let mut w = XdrW::new();

                if let Some(p) = path_from_fh(&export_root, &fh) {
                    if let Ok(meta) = fs::metadata(&p) {
                        w.put_u32(0);
                        xdr_fattr(&mut w, &meta);
                    } else {
                        w.put_u32(2);
                    }
                } else {
                    w.put_u32(2);
                }

                rpc_accept_reply(call.xid, 0, &w.buf)
            }

            4 => {
                // LOOKUP(dirfh, name)
                let dirfh = r.get_opaque().unwrap_or_default();
                let name = r.get_string().unwrap_or_default();
                let mut w = XdrW::new();

                if let Some(dir) = path_from_fh(&export_root, &dirfh) {
                    let path = dir.join(&name);
                    if let Ok(meta) = fs::metadata(&path) {
                        w.put_u32(0);
                        w.put_opaque(&fh_from_path(&path.to_string_lossy()));
                        xdr_fattr(&mut w, &meta);
                    } else {
                        w.put_u32(2);
                    }
                } else {
                    w.put_u32(2);
                }

                rpc_accept_reply(call.xid, 0, &w.buf)
            }

            6 => {
                // READ(fh, offset, count, totalcount)
                let fh = r.get_opaque().unwrap_or_default();
                let offset = r.get_u32().unwrap_or(0) as u64;
                let count = r.get_u32().unwrap_or(0) as usize;
                let _total = r.get_u32().unwrap_or(0);

                let mut w = XdrW::new();

                if let Some(p) = path_from_fh(&export_root, &fh) {
                    if let Ok(mut f) = fs::File::open(&p) {
                        use std::io::{Read, Seek};
                        let _ = f.seek(io::SeekFrom::Start(offset));
                        let mut tmp = vec![0u8; count.min(8192)];
                        let n = f.read(&mut tmp).unwrap_or(0);

                        w.put_u32(0);
                        w.put_u32((offset + n as u64) as u32);
                        w.put_opaque(&tmp[..n]);
                    } else {
                        w.put_u32(2);
                    }
                } else {
                    w.put_u32(2);
                }

                rpc_accept_reply(call.xid, 0, &w.buf)
            }

            16 => {
                // READDIR(fh, cookie, count)
                let fh = r.get_opaque().unwrap_or_default();
                let cookie = r.get_u32().unwrap_or(0);
                let _count = r.get_u32().unwrap_or(0);

                let mut w = XdrW::new();

                if let Some(p) = path_from_fh(&export_root, &fh) {
                    if let Ok(read) = fs::read_dir(&p) {
                        w.put_u32(0);
                        let mut idx = 0u32;

                        for e in read.flatten() {
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
                        w.put_u32(2);
                    }
                } else {
                    w.put_u32(2);
                }

                rpc_accept_reply(call.xid, 0, &w.buf)
            }

            17 => {
                // STATFS
                let _fh = r.get_opaque().unwrap_or_default();
                let mut w = XdrW::new();
                w.put_u32(0);
                w.put_u32(4096);
                w.put_u32(4096);
                w.put_u32(1024);
                w.put_u32(512);
                w.put_u32(512);
                rpc_accept_reply(call.xid, 0, &w.buf)
            }

            p => {
                // Unsupported procedure
                warn!(procid = p, "nfs: unsupported proc");
                let w = XdrW::new();
                rpc_accept_reply(call.xid, 0, &w.buf)
            }
        };

        Some(reply)
    }

    /// UDP server
    pub async fn run_udp(self, sock: UdpSocket) {
        let local = sock.local_addr().ok();
        info!(?local, "nfsd listening (UDP)");

        let mut buf = vec![0u8; 32768];

        loop {
            let Ok((n, peer)) = sock.recv_from(&mut buf).await else {
                continue;
            };

            if let Some(reply) = self.handle_call(&buf[..n]) {
                if let Err(e) = sock.send_to(&reply, peer).await {
                    warn!(?e, %peer, "nfsd UDP send failed");
                }
            }
        }
    }

    /// TCP server (record-marked RPC)
    pub async fn run_tcp(self, listener: TcpListener) {
        let local = listener.local_addr().ok();
        info!(?local, "nfsd listening (TCP)");

        loop {
            let (mut stream, peer) = match listener.accept().await {
                Ok(v) => v,
                Err(e) => {
                    warn!(?e, "nfsd TCP accept failed");
                    continue;
                }
            };

            let this = self.clone();

            tokio::spawn(async move {
                info!(%peer, "nfsd TCP connected");

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

                    if let Some(reply) = this.handle_call(&buf) {
                        let mut out = Vec::with_capacity(4 + reply.len());
                        out.extend_from_slice(
                            &(0x8000_0000u32 | (reply.len() as u32)).to_be_bytes(),
                        );
                        out.extend_from_slice(&reply);

                        if stream.write_all(&out).await.is_err() {
                            break;
                        }
                    } else {
                        break;
                    }
                }

                info!(%peer, "nfsd TCP disconnected");
            });
        }
    }
}
