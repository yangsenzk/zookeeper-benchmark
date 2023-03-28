mkdir -p output

cargo build --release
cp ./target/release/zookeeper-benchmark ./output/zookeeper-benchmark