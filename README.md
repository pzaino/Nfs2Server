# README.md

nfs2-rs is a tiny, educational NFSv2 read only server for retro clients. It implements UDP SunRPC, mount v1, and a small subset of NFSv2. It is not production ready. It exists to help you share files with 80s and 90s machines that only speak NFS v2.

## How to build

Requires Rust and Cargo. Tested on Linux.

First install Rust from https://rustup.rs/

Then clone and build:

```sh
git clone git@github.com:pzaino/Nfs2Server.git
cd Nfs2Server
cargo build --release
```

Quick start:

1) edit `exports.toml` to add your export paths

2) run the server with `./target/release/nfs2-rs`

3) Test on Linux:

    ```sh
    # on a Linux client with v2 available
    sudo mount -t nfs -o vers=2,proto=udp,port=<nfsd_port>,mountport=<mountd_port> <server_ip>:/tmp/nfs_export /mnt
    ```

Design notes:

1. File handles encode dev, ino, mode, uid, gid. Lookup resolves handles by ino under the export root with a naive scan. Good enough for small shares.
2. Only read related calls are supported. Write family returns error.
3. No auth beyond AUTH_NULL. Use a sandbox or run in a container. Add IP based export filters if needed.
4. rpcbind is a stub that returns 0. Pass explicit ports in the client mount command.

Roadmap:

- Add AUTH UNIX cred parsing and uid map
- Support WRITE, CREATE, REMOVE with a read write flag
- Use a walking inode table for faster fh lookup
- Implement proper rpcbind GETPORT and dynamic registration
- Add configuration file with exports and client ACLs

## LICENSE

MPL 2.0 (Mozilla Public License Version 2.0). See [LICENSE](LICENSE.txt) file for details.
