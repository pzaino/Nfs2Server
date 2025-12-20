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
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, UdpSocket},
};
use tracing::{debug, info, warn};

// NFS v2 program 100003, version 2
// Minimal-ish set: NULL(0), GETATTR(1), ROOT(3), LOOKUP(4), READLINK(5), READ(6),
// WRITECACHE(7), READDIR(16), STATFS(17)

const NFS_PROG: u32 = 100003;
const NFS_VERS: u32 = 2;

// NFS status
const NFS_OK: u32 = 0;
const NFSERR_PERM: u32 = 1;
const NFSERR_NOENT: u32 = 2;
const NFSERR_ACCES: u32 = 13;
const NFSERR_INVAL: u32 = 22;
const NFSERR_NOTDIR: u32 = 20;

#[derive(Clone)]
pub struct Nfs2 {
    exports: Exports,
}

impl Nfs2 {
    pub fn new(exports: Exports) -> Self {
        Self { exports }
    }
}

fn log_rpc_probe(buf: &[u8], peer: &std::net::SocketAddr) {
    use crate::xdr::XdrR;

    let mut r = XdrR::new(buf);

    let xid = r.get_u32().ok();
    let mtype = r.get_u32().ok();
    let rpcvers = r.get_u32().ok();
    let prog = r.get_u32().ok();
    let vers = r.get_u32().ok();
    let procid = r.get_u32().ok();

    tracing::info!(
        %peer,
        xid = ?xid,
        mtype = ?mtype,
        rpcvers = ?rpcvers,
        prog = ?prog,
        vers = ?vers,
        procid = ?procid,
        "nfs2 TCP probe"
    );
}

/// 32-byte-ish file handle encoding:
/// [dev_hi u32 | dev_lo u32 | ino_hi u32 | ino_lo u32 | pad...]
///
/// Keep it deterministic and stable.
pub fn fh_from_path(path: &Path) -> Vec<u8> {
    let meta = fs::symlink_metadata(path).ok();

    let (dev, ino) = if let Some(m) = meta {
        (m.dev() as u64, m.ino() as u64)
    } else {
        (0u64, 0u64)
    };

    let mut w = XdrW::new();
    w.put_u32((dev >> 32) as u32);
    w.put_u32(dev as u32);
    w.put_u32((ino >> 32) as u32);
    w.put_u32(ino as u32);

    // pad to 32 bytes (clients often assume fixed-ish length for v2 fh)
    let mut v = w.buf.to_vec();
    v.resize(32, 0);
    v
}

fn u64_from_be_u32s(hi: u32, lo: u32) -> u64 {
    ((hi as u64) << 32) | (lo as u64)
}

fn parse_fh_dev_ino(fh: &[u8]) -> Option<(u64, u64)> {
    if fh.len() < 16 {
        return None;
    }
    let dev_hi = u32::from_be_bytes(fh[0..4].try_into().ok()?);
    let dev_lo = u32::from_be_bytes(fh[4..8].try_into().ok()?);
    let ino_hi = u32::from_be_bytes(fh[8..12].try_into().ok()?);
    let ino_lo = u32::from_be_bytes(fh[12..16].try_into().ok()?);

    let dev = u64_from_be_u32s(dev_hi, dev_lo);
    let ino = u64_from_be_u32s(ino_hi, ino_lo);
    Some((dev, ino))
}

fn secs_from_i64(v: i64) -> u32 {
    if v <= 0 {
        0
    } else if v as u128 > u32::MAX as u128 {
        u32::MAX
    } else {
        v as u32
    }
}

/// NFSv2 fattr (RFC1094)
fn xdr_fattr(w: &mut XdrW, meta: &fs::Metadata) {
    // ftype: 1=NFREG, 2=NFDIR, 5=NFLNK (we only do file/dir-ish)
    let ftype = if meta.is_dir() { 2 } else { 1 };

    w.put_u32(ftype);
    w.put_u32(meta.mode());
    w.put_u32(meta.nlink() as u32);
    w.put_u32(meta.uid());
    w.put_u32(meta.gid());
    w.put_u32(meta.size() as u32);
    // blocksize, rdev, blocks
    w.put_u32(4096);
    w.put_u32(meta.rdev() as u32);
    w.put_u32(((meta.size() + 511) / 512) as u32);

    // fsid + fileid (simple mapping)
    w.put_u32(meta.dev() as u32);
    w.put_u32(meta.ino() as u32);

    // atime, mtime, ctime: (seconds, useconds)
    w.put_u32(secs_from_i64(meta.atime()));
    w.put_u32(0);
    w.put_u32(secs_from_i64(meta.mtime()));
    w.put_u32(0);
    w.put_u32(secs_from_i64(meta.ctime()));
    w.put_u32(0);
}

/// Recursively find inode inside a root.
/// This is slow, but acceptable for “get it working” and small exports.
/// If performance becomes a problem, we can add a cache (ino -> path) per export root.
fn find_by_ino(base: &Path, target_ino: u64) -> Option<PathBuf> {
    let meta = fs::symlink_metadata(base).ok()?;
    if meta.ino() as u64 == target_ino {
        return Some(base.to_path_buf());
    }
    if meta.is_dir() {
        let rd = fs::read_dir(base).ok()?;
        for ent in rd.flatten() {
            let p = ent.path();
            if let Some(found) = find_by_ino(&p, target_ino) {
                return Some(found);
            }
        }
    }
    None
}

fn export_roots(exports: &Exports) -> Vec<PathBuf> {
    exports.list().iter().map(|e| e.path.clone()).collect()
}

/// Convert filehandle to an actual path by searching each export root.
fn path_from_fh(exports: &Exports, fh: &[u8]) -> Option<PathBuf> {
    let (_dev, ino) = parse_fh_dev_ino(fh)?;
    // For now, only inode lookup. We could also filter by dev for speed/accuracy.
    for root in export_roots(exports) {
        if let Some(p) = find_by_ino(&root, ino) {
            return Some(p);
        }
    }
    None
}

/// Ensure a child path stays within one of the exports.
/// This prevents ".." traversal.
fn within_exports(exports: &Exports, p: &Path) -> bool {
    for root in export_roots(exports) {
        if p.starts_with(&root) {
            return true;
        }
    }
    false
}

impl Nfs2 {
    /// Transport-independent NFSv2 handler.
    pub fn handle_call(&self, buf: &[u8]) -> Option<Vec<u8>> {
        let (call, ofs) = decode_call(buf)?;

        if call.prog != NFS_PROG || call.vers != NFS_VERS {
            return None;
        }

        let mut r = XdrR::new(&buf[ofs..]);

        info!(xid = call.xid, procid = call.procid, "nfs2: request");

        let reply = match call.procid {
            // NULL
            0 => {
                let w = XdrW::new();
                rpc_accept_reply(call.xid, NFS_OK, &w.buf)
            }

            // GETATTR(fh) -> (status, fattr)
            1 => {
                let fh = r.get_opaque().unwrap_or_default();
                let mut w = XdrW::new();

                if let Some(p) = path_from_fh(&self.exports, &fh) {
                    if let Ok(meta) = fs::symlink_metadata(&p) {
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

            // ROOT() -> historically unused. Some clients still call it.
            3 => {
                let w = XdrW::new();
                rpc_accept_reply(call.xid, NFS_OK, &w.buf)
            }

            // LOOKUP(dirfh, name) -> (status, fh, fattr)
            4 => {
                let dirfh = r.get_opaque().unwrap_or_default();
                let name = r.get_string().unwrap_or_default();
                let mut w = XdrW::new();

                if let Some(dir) = path_from_fh(&self.exports, &dirfh) {
                    let child = dir.join(&name);
                    if !within_exports(&self.exports, &child) {
                        w.put_u32(NFSERR_ACCES);
                    } else if let Ok(meta) = fs::symlink_metadata(&child) {
                        w.put_u32(NFS_OK);
                        let fh = fh_from_path(&child);
                        w.put_opaque(&fh);
                        xdr_fattr(&mut w, &meta);
                    } else {
                        w.put_u32(NFSERR_NOENT);
                    }
                } else {
                    w.put_u32(NFSERR_NOENT);
                }

                rpc_accept_reply(call.xid, NFS_OK, &w.buf)
            }

            // READLINK(fh) -> (status, path)
            5 => {
                let fh = r.get_opaque().unwrap_or_default();
                let mut w = XdrW::new();

                if let Some(p) = path_from_fh(&self.exports, &fh) {
                    match fs::read_link(&p) {
                        Ok(t) => {
                            w.put_u32(NFS_OK);
                            w.put_string(&t.to_string_lossy());
                        }
                        Err(_) => {
                            // not a symlink or can't read
                            w.put_u32(NFSERR_INVAL);
                        }
                    }
                } else {
                    w.put_u32(NFSERR_NOENT);
                }

                rpc_accept_reply(call.xid, NFS_OK, &w.buf)
            }

            // READ(fh, offset, count, totalcount) -> (status, fattr, data)
            6 => {
                let fh = r.get_opaque().unwrap_or_default();
                let offset = r.get_u32().unwrap_or(0) as u64;
                let count = r.get_u32().unwrap_or(0) as usize;
                let _total = r.get_u32().unwrap_or(0);

                let mut w = XdrW::new();

                if let Some(p) = path_from_fh(&self.exports, &fh) {
                    if !within_exports(&self.exports, &p) {
                        w.put_u32(NFSERR_ACCES);
                    } else if let Ok(meta) = fs::symlink_metadata(&p) {
                        if meta.is_dir() {
                            w.put_u32(NFSERR_PERM);
                        } else if let Ok(mut f) = fs::File::open(&p) {
                            use std::io::{Read, Seek};
                            if f.seek(io::SeekFrom::Start(offset)).is_err() {
                                w.put_u32(NFSERR_INVAL);
                            } else {
                                let mut tmp = vec![0u8; count.min(64 * 1024)];
                                let n = f.read(&mut tmp).unwrap_or(0);

                                w.put_u32(NFS_OK);
                                xdr_fattr(&mut w, &meta);

                                // IMPORTANT: opaque already contains length, do NOT add an extra length u32 here.
                                w.put_opaque(&tmp[..n]);
                            }
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

            // WRITECACHE() -> void (accept OK)
            7 => {
                let w = XdrW::new();
                rpc_accept_reply(call.xid, NFS_OK, &w.buf)
            }

            // READDIR(fh, cookie, count) -> (status, entries, eof)
            16 => {
                let fh = r.get_opaque().unwrap_or_default();
                let cookie = r.get_u32().unwrap_or(0);
                let _count = r.get_u32().unwrap_or(0);

                let mut w = XdrW::new();

                if let Some(dir) = path_from_fh(&self.exports, &fh) {
                    if let Ok(meta) = fs::symlink_metadata(&dir) {
                        if !meta.is_dir() {
                            w.put_u32(NFSERR_NOTDIR);
                        } else if let Ok(rd) = fs::read_dir(&dir) {
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
                } else {
                    w.put_u32(NFSERR_NOENT);
                }

                rpc_accept_reply(call.xid, NFS_OK, &w.buf)
            }

            // STATFS(fh) -> (status, tsize,bsize,blocks,bfree,bavail)
            17 => {
                let _fh = r.get_opaque().unwrap_or_default();
                let mut w = XdrW::new();
                w.put_u32(NFS_OK);
                w.put_u32(8192); // tsize
                w.put_u32(4096); // bsize
                w.put_u32(1024); // blocks (dummy)
                w.put_u32(512); // bfree  (dummy)
                w.put_u32(512); // bavail (dummy)
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

    /// Runs the NFS server over UDP
    pub async fn run_udp(self, sock: UdpSocket) {
        let local = sock.local_addr().ok();
        info!(?local, "nfsd listening (UDP)");

        let mut buf = vec![0u8; 64 * 1024];

        loop {
            let Ok((n, peer)) = sock.recv_from(&mut buf).await else {
                continue;
            };

            debug!(%peer, size = n, "nfs2 UDP packet");

            if let Some(r) = self.handle_call(&buf[..n]) {
                if let Err(e) = sock.send_to(&r, peer).await {
                    warn!(?e, %peer, "nfs2 UDP send failed");
                }
            }
        }
    }

    /// Runs the NFS server over TCP (record-marked RPC, supports multi-fragment)
    pub async fn run_tcp(self, listener: TcpListener) {
        let local = listener.local_addr().ok();
        info!(?local, "nfsd listening (TCP)");

        loop {
            let (mut stream, peer) = match listener.accept().await {
                Ok(v) => v,
                Err(e) => {
                    warn!(?e, "nfs2 TCP accept failed");
                    continue;
                }
            };

            let this = self.clone();

            tokio::spawn(async move {
                info!(%peer, "nfs2 TCP connected");

                loop {
                    // Reassemble a full RPC message (may be fragmented).
                    let mut full = Vec::<u8>::new();

                    loop {
                        let mut hdr = [0u8; 4];
                        if stream.read_exact(&mut hdr).await.is_err() {
                            info!(%peer, "nfs2 TCP disconnected");
                            return;
                        }

                        let marker = u32::from_be_bytes(hdr);
                        let last = (marker & 0x8000_0000) != 0;
                        let len = (marker & 0x7fff_ffff) as usize;

                        if len == 0 {
                            // empty fragment is odd but ignore
                            if last {
                                break;
                            }
                            continue;
                        }

                        let mut frag = vec![0u8; len];
                        if stream.read_exact(&mut frag).await.is_err() {
                            info!(%peer, "nfs2 TCP disconnected");
                            return;
                        }

                        full.extend_from_slice(&frag);

                        if last {
                            break;
                        }

                        if full.len() > 4 * 1024 * 1024 {
                            warn!(%peer, size = full.len(), "nfs2 TCP message too large, dropping");
                            return;
                        }
                    }

                    log_rpc_probe(&full, &peer);

                    if let Some(reply) = this.handle_call(&full) {
                        let mut out = Vec::with_capacity(4 + reply.len());
                        out.extend_from_slice(&(0x8000_0000u32 | reply.len() as u32).to_be_bytes());
                        out.extend_from_slice(&reply);

                        if stream.write_all(&out).await.is_err() {
                            info!(%peer, "nfs2 TCP disconnected");
                            return;
                        }
                    } else {
                        // IMPORTANT: do NOT close connection
                        debug!(%peer, "nfs2 TCP: ignoring non-NFS RPC");
                        continue;
                    }
                }
            });
        }
    }
}
