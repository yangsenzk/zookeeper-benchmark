mkdir -p output
rm -rf output/*
cargo build --release
cp ./target/release/zookeeper-benchmark ./output/zookeeper-benchmark