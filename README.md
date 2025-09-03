# qmdb-go

Go bindings for [QMDB](https://github.com/LayerZero-Labs/qmdb), a high-performance verifiable key-value store.

## Building

1. Build QMDB FFI bindings

   ```bash
   git clone https://github.com/minhd-vu/qmdb
   cargo build --release
   ```

2. Build Go package

   ```bash
   export CGO_LDFLAGS="-L/path_to_qmdb/target/release"
   go build
   ```
