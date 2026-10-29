# Deploying the benchmark server to Kubernetes

No registry push is needed: the pod runs the public `eclipse-temurin:21-jre` image and mounts the jars from a ConfigMap
(the jars total about 300 KB, well under the 1 MiB ConfigMap limit).

```sh
mvn -q compile
# classes must target Java 21, the image's runtime
javac --release 21 -d /tmp/b21 -cp target/classes benchmark/src/servers/HydrangeaHttpServer.java benchmark/src/bench/HttpLoad.java benchmark/src/bench/Chaos.java
jar cf /tmp/hydrangea.jar -C target/classes . ; jar cf /tmp/bench.jar -C /tmp/b21 .
# use `create`, not `apply`: apply stores the whole jar in an annotation and exceeds its size limit
kubectl -n NS create configmap hydrangea-jars --from-file=/tmp/hydrangea.jar --from-file=/tmp/bench.jar
kubectl -n NS label configmap hydrangea-jars app.kubernetes.io/part-of=hydrangea-bench
kubectl apply -f k8s/server.yaml            # edit the namespace first
kubectl -n NS port-forward svc/hydrangea-server 18080:8080
kubectl apply -f k8s/load-job.yaml          # in-cluster load
kubectl apply -f k8s/rollout-job.yaml       # traffic while you run: kubectl rollout restart deploy/hydrangea-server
# clean up: everything carries the label
kubectl -n NS delete deploy,svc,cm,job,pod -l app.kubernetes.io/part-of=hydrangea-bench
```

`server.yaml` has a `preStop` sleep so the Service stops routing new connections to a pod before it begins its graceful shutdown.

## PostgreSQL client load (Hydrangea vs pgjdbc)

`benchmark/run-k8s-pg.sh` builds the jars, creates three ConfigMaps (pgjdbc is over 1 MiB, so it is in two halves), a Secret with the database login (given as `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDB`
in the environment and written nowhere else), runs `k8s/pg-load.yaml.tmpl` as a Job, copies its log out and deletes everything carrying the label `app.kubernetes.io/part-of=hydrangea-bench`.
It runs read-only scenarios against at most 32 connections by default; `insert` and `tx` (and `point`, `rows`, which need the tables of `PgBench setup`) are refused unless `ALLOW_WRITES=yes`.
