#!/usr/bin/env bash
# Usage: benchmark/profile-grpc.sh [server=hydrangea|java] [client=java|hydrangea] [scenario=unary-16] [inflight=64] [channels=4]
# JFR CPU samples of the server (leaf frames) under the load of GrpcBench; for finding hot spots, not for comparing numbers.
cd "$(dirname "$0")/.."
SRV=${1:-hydrangea}; CLI=${2:-java}; SCN=${3:-unary-16}; INF=${4:-64}; CH=${5:-4}
O=$PWD/benchmark/out-grpc; CP="${O}:target/classes:$(cat ${O}/cp.txt)"
mvn -q -o compile && javac --release 21 -d "${O}" -cp "target/classes:$(cat ${O}/cp.txt)" benchmark/src/grpc/GrpcBench.java || exit 1
java -Xmx1g -XX:StartFlightRecording=filename=${O}/srv.jfr,settings=profile -cp "$CP" GrpcBench server $SRV 0 4 > ${O}/s.log 2>&1 & SP=$!
for i in $(seq 50); do grep -q READY ${O}/s.log && break; sleep 0.2; done; PORT=$(sed -n 's/READY \([0-9]*\) .*/\1/p' ${O}/s.log)
java -cp "$CP" GrpcBench load $CLI $PORT $SCN $INF $CH 15 $SP 2>/dev/null
jcmd $SP JFR.dump filename=${O}/srv.jfr >/dev/null; kill $SP; wait $SP 2>/dev/null
echo "--- leaf frames"; jfr print --events jdk.ExecutionSample ${O}/srv.jfr | grep -A1 "stackTrace = \[" | grep -v "stackTrace\|^--" | sed 's/line:.*//' | sort | uniq -c | sort -rn | head -${TOP:-22}
