// src/nfs2.rs

use crate::export::Exports;
use crate::mountd::MountTable;
use crate::rpc::{decode_call, rpc_accept_reply, rpc_prog_mismatch_reply};
use crate::xdr::{XdrR, XdrW};
use hex;
//use tracing_subscriber::field::debug;

use std::{
    fs,
    //io::{Read, Seek},
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UdpSocket};
use tracing::{debug, info, warn};

const NFS_PROG: u32 = 100003;
const NFS_VERS: u32 = 2;

// NFSv2 status codes
const NFS_OK: u32 = 0;
const NFSERR_NOENT: u32 = 2;
const NFSERR_ACCES: u32 = 13;
const NFSERR_STALE: u32 = 70;

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
    info!("nfs2: path_from_fh fh_hex={}", hex::encode(fh));
    if fh.len() != 32 {
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

fn nfs_err(errcode: u32) -> Vec<u8> {
    let mut w = XdrW::new();
    w.put_u32(errcode);
    w.buf.to_vec()
}

// ------------------------------------------------------------
// XDR helpers
// ------------------------------------------------------------

fn put_fattr(w: &mut XdrW, meta: &std::fs::Metadata, path: &Path) {
    use std::os::unix::fs::MetadataExt;

    let is_dir = meta.is_dir();

    // --- ftype ---
    let ftype = if is_dir { 2 } else { 1 }; // NFDIR = 2, NFREG = 1
    w.put_u32(ftype);

    // --- mode ---
    let mut mode = meta.mode() & 0o777;
    if is_dir {
        mode |= 0o040000;
    } else {
        mode |= 0o100000;
    }
    w.put_u32(mode);

    // --- nlink ---
    let nlink = if is_dir { 2 } else { meta.nlink() as u32 };
    w.put_u32(nlink);

    // --- uid / gid ---
    w.put_u32(meta.uid());
    w.put_u32(meta.gid());

    // --- size ---
    let size = if is_dir { 512 } else { meta.len() as u32 };
    w.put_u32(size);

    // --- blocksize ---
    w.put_u32(512);

    // --- rdev ---
    w.put_u32(0);

    // --- blocks ---
    let blocks = if is_dir {
        1
    } else {
        ((meta.len() + 511) / 512) as u32
    };
    w.put_u32(blocks);

    // --- fsid ---
    w.put_u32(1);

    // --- fileid (DO NOT USE inode) ---
    let fileid = crc32fast::hash(path.to_string_lossy().as_bytes());
    w.put_u32(fileid);

    // --- times ---
    let atime = meta.atime() as u32;
    let mtime = meta.mtime() as u32;
    let ctime = meta.ctime() as u32;

    w.put_u32(atime);
    w.put_u32(0);
    w.put_u32(mtime);
    w.put_u32(0);
    w.put_u32(ctime);
    w.put_u32(0);
    // log all fattr fields
    debug!(
        path = %path.display(),
        ftype,
        mode = format_args!("{:o}", mode),
        nlink,
        uid = meta.uid(),
        gid = meta.gid(),
        size,
        blocks,
        fileid,
        atime,
        mtime,
        ctime,
        "nfs2: file attributes"
    );
}

// ------------------------------------------------------------

#[derive(Clone)]
pub struct Nfs2 {
    exports: Exports,
    mounts: MountTable,
}

impl Nfs2 {
    pub fn new(exports: Exports, mounts: MountTable) -> Self {
        Self { exports, mounts }
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
                let mut fh = r.get_opaque().unwrap_or_default();

                if fh.is_empty() {
                    if let Some((_, root_fh)) = self.mounts.lock().unwrap().iter().next() {
                        fh = root_fh.clone();
                    } else {
                        return Some(nfs_err(NFSERR_STALE));
                    }
                }
                let mut w = XdrW::new();

                info!(
                    "nfs2: GETATTR raw file handle fh_len={}, fh_hex={}",
                    fh.len(),
                    hex::encode(&fh)
                );
                if let Some(p) = path_from_fh(root, &fh) {
                    if let Ok(meta) = fs::metadata(&p) {
                        info!(
                            peer,
                            path = %p.display(),
                            size = meta.len(),
                            ino = meta.ino(),
                            mode = format_args!("{:o}", meta.mode()),
                            "nfs2: GETATTR metadata"
                        );
                        w.put_u32(NFS_OK);
                        put_fattr(&mut w, &meta, &p);
                    } else {
                        w.put_u32(NFSERR_NOENT);
                        // Log meta failure
                        info!(peer, path = %p.display(), "nfs2: GETATTR metadata failed");
                    }
                } else {
                    w.put_u32(NFSERR_NOENT);
                }

                rpc_accept_reply(call.xid, 0, &w.buf)
            }

            // LOOKUP
            4 => {
                info!(
                    peer,
                    vers = call.vers,
                    auth = ?call.auth,
                    "nfs2: LOOKUP entered"
                );
                let dirfh = r.get_opaque().unwrap_or_default();
                let name = r.get_string().unwrap_or_default();
                let mut w = XdrW::new();

                info!(
                    peer,
                    "nfs2: LOOKUP start fh_len={} fh_hex={} name='{}'",
                    dirfh.len(),
                    hex::encode(&dirfh),
                    name
                );

                if let Some(dir) = path_from_fh(root, &dirfh) {
                    let p = dir.join(&name);

                    info!(
                        peer,
                        "nfs2: LOOKUP resolved dir='{}' path='{}'",
                        dir.display(),
                        p.display()
                    );

                    if let Ok(meta) = fs::metadata(&p) {
                        info!(
                            peer,
                            "nfs2: LOOKUP success path='{}' mode={:o} ino={}",
                            p.display(),
                            meta.mode(),
                            meta.ino()
                        );

                        w.put_u32(NFS_OK);
                        w.put_opaque(&fh_from_path(&p));
                        put_fattr(&mut w, &meta, &p);
                    } else {
                        info!(peer, "nfs2: LOOKUP metadata failed path='{}'", p.display());
                        w.put_u32(NFSERR_NOENT);
                    }
                } else {
                    info!(
                        peer,
                        "nfs2: LOOKUP invalid dirfh fh_hex={}",
                        hex::encode(&dirfh)
                    );
                    w.put_u32(NFSERR_NOENT);
                }

                info!(peer, "nfs2: LOOKUP end");

                rpc_accept_reply(call.xid, 0, &w.buf)
            }

            // READDIR
            16 => {
                let mut fh = r.get_opaque().unwrap_or_default();

                if fh.is_empty() {
                    if let Some((_, root_fh)) = self.mounts.lock().unwrap().iter().next() {
                        fh = root_fh.clone();
                    } else {
                        return Some(nfs_err(NFSERR_STALE));
                    }
                }

                let cookie = r.get_u32().unwrap_or(0);
                let count = r.get_u32().unwrap_or(0) as usize;

                let mut w = XdrW::new();

                info!(
                    "nfs2: READDIR raw file handle fh_len={}, fh_hex={}",
                    fh.len(),
                    hex::encode(&fh)
                );
                if let Some(dir) = path_from_fh(root, &fh) {
                    if let Ok(rd) = fs::read_dir(&dir) {
                        w.put_u32(NFS_OK);

                        // If client sends 0, pick a sane cap to avoid giant replies.
                        // RISC OS can be quite sensitive here.
                        let max_bytes = if count == 0 { 4096 } else { count };

                        let mut idx = 0u32;
                        let mut eof = true;

                        for e in rd.flatten() {
                            if idx < cookie {
                                idx += 1;
                                continue;
                            }

                            let name = e.file_name().to_string_lossy().into_owned();
                            let ino = e.metadata().map(|m| m.ino() as u32).unwrap_or(0);

                            // Estimate how many bytes this entry will add in XDR.
                            // entry = bool(4) + fileid(4) + string(len+pad+4) + cookie(4)
                            // string encoding = u32 len + bytes + padding
                            let name_len = name.as_bytes().len();
                            let name_pad = (4 - (name_len % 4)) % 4;
                            let entry_bytes = 4 + 4 + (4 + name_len + name_pad) + 4;

                            // +8 for end markers (final 0 + eof bool) to keep room
                            if w.buf.len() + entry_bytes + 8 > max_bytes {
                                eof = false;
                                break;
                            }

                            w.put_u32(1); // entry follows
                            w.put_u32(ino); // fileid
                            w.put_string(&name); // filename
                            w.put_u32(idx + 1); // cookie for next call
                            idx += 1;
                        }

                        w.put_u32(0); // end of entry list
                        w.put_u32(if eof { 1 } else { 0 }); // EOF flag
                    } else {
                        w.put_u32(NFSERR_NOENT);
                    }
                } else {
                    w.put_u32(NFSERR_STALE);
                }
                info!(
                    peer,
                    cookie,
                    count,
                    reply_size = w.buf.len(),
                    "nfs2: READDIR reply"
                );
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
