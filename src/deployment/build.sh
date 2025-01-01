RUST_BACKTRACE=1 DISK_STORAGE=/mnt/lambda cargo lambda build --release --bin idk-lambda && cargo lambda deploy --binary-name idk-lambda
