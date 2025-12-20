// src/main.rs

use anyhow::Result;
use std::fs;
use std::path::{Path, PathBuf};
use tokio::net::UdpSocket;
use tokio::signal;
use tracing::{debug, info, warn};

mod export;
mod mountd;
mod nfs2;
mod rpc;
mod xdr;

use crate::export::{Export, Exports};
use serde::Deserialize;

//
// ---- TOML exports parsing ----
//

#[derive(Debug, Deserialize)]
struct ExportsFile {
    export: Vec<ExportEntry>,
}

#[derive(Debug, Deserialize)]
struct ExportEntry {
    path: PathBuf,

    #[serde(default)]
    read_only: bool,

    #[serde(default = "default_anon_uid")]
    anon_uid: u32,

    #[serde(default = "default_anon_gid")]
    anon_gid: u32,

    #[serde(default)]
    clients: Vec<String>,
}

fn default_anon_uid() -> u32 {
    65534
}
fn default_anon_gid() -> u32 {
    65534
}

fn load_exports(path: &str) -> Result<Exports> {
    debug!(path, "checking exports file");

    if !Path::new(path).exists() {
        debug!(path, "exports file not found");
        return Ok(Exports::new(Vec::new()));
    }

    info!(path, "reading exports file");

    let data = fs::read_to_string(path)?;
    let parsed: ExportsFile = toml::from_str(&data)?;

    let exports = parsed
        .export
        .into_iter()
        .map(|e| Export {
            path: e.path,
            read_only: e.read_only,
            anon_uid: e.anon_uid,
            anon_gid: e.anon_gid,
            clients: e.clients,
        })
        .collect::<Vec<_>>();

    debug!(
        count = exports.len(),
        exports = ?exports,
        "exports parsed successfully"
    );

    Ok(Exports::new(exports))
}

//
// ---- main ----
//

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    info!("Nfs2Server starting");

    //
    // ---- Load exports ----
    //

    let exports_path = "./exports.toml";
    info!(path = exports_path, "loading exports");

    let exports = load_exports(exports_path)?;

    if exports.list().is_empty() {
        warn!("no exports configured (file missing or empty)");
    } else {
        info!(
            count = exports.list().len(),
            path = exports_path,
            "exports loaded"
        );
    }

    //
    // ---- Initialise services ----
    //

    debug!("initialising mountd");
    let mountd = mountd::Mountd::new(exports.clone());

    debug!("initialising nfsd");
    let nfsd = nfs2::Nfs2::new(exports);

    //
    // ---- Bind sockets explicitly ----
    //

    info!("binding UDP sockets");

    let mountd_socket = UdpSocket::bind("0.0.0.0:0").await?;
    let mountd_port = mountd_socket.local_addr()?.port();
    info!(mountd_port, "mountd socket bound");

    let nfs_socket = UdpSocket::bind("0.0.0.0:0").await?;
    let nfs_port = nfs_socket.local_addr()?.port();
    info!(nfs_port, "nfsd socket bound");

    //
    // ---- Register with rpcbind ----
    //

    info!("registering services with rpcbind");

    rpc::rpcbind_register_udp(100005, 1, mountd_port).await?;
    info!(
        program = 100005,
        version = 1,
        port = mountd_port,
        "mountd registered with rpcbind"
    );

    rpc::rpcbind_register_udp(100003, 2, nfs_port).await?;
    info!(
        program = 100003,
        version = 2,
        port = nfs_port,
        "nfsd registered with rpcbind"
    );

    //
    // ---- Start servers ----
    //

    info!("starting service tasks");

    tokio::spawn(async move {
        info!("mountd task started");
        mountd.run(mountd_socket, 100005, 1).await;
        warn!("mountd task exited");
    });

    tokio::spawn(async move {
        info!("nfsd task started");
        nfsd.run(nfs_socket, 100003, 2).await;
        warn!("nfsd task exited");
    });

    info!("nfs2-rs started successfully");
    info!("waiting for Ctrl+C");

    //
    // ---- Shutdown ----
    //

    signal::ctrl_c().await?;
    info!("shutdown signal received");

    Ok(())
}
