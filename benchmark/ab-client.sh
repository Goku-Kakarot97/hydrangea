#!/usr/bin/env bash
# Usage: benchmark/ab-client.sh scenario inflight channels [rounds=4] [seconds=10]
# Client A/B against one grpc-java server: Hydrangea's client and grpc-java's client alternate, so drift hits both equally.
cd "$(dirname "$0")/.."
SCN=$1; INF=$2; CH=$3; ROUNDS=${4:-4}; SECS=${5:-10}
O=$PWD/benchmark/out-grpc; CP="${O}:target/classes:$(cat ${O}/cp.txt)"
java -Xmx1g -cp "$CP" GrpcBench server java 0 4 > $O/s.log 2>&1 & SP=$!
for i in $(seq 50); do grep -q READY $O/s.log && break; sleep 0.2; done
PORT=$(sed -n 's/READY \([0-9]*\) .*/\1/p' $O/s.log)
echo "# $SCN inflight=$INF channels=$CH, server grpc-java"
for r in $(seq $ROUNDS); do for C in java hydrangea; do
  java -Xmx1g -cp "$CP" GrpcBench load $C $PORT $SCN $INF $CH $SECS $SP 2>/dev/null | python3 -c "import sys,re;[print(re.sub(r'RESULT impl=(\w+) .* rps=(\d+).*p99=(\d+).*serverCpuUsPerOp=([\d.]+) clientCpuUsPerOp=([\d.]+).*',r'\1 rps=\2 p99=\3 serverCpu=\4 clientCpu=\5',l.strip())) for l in sys.stdin]"
done; done
kill $SP; wait $SP 2>/dev/null
