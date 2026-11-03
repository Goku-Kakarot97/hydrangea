# Build from the repository root after `mvn -q -DskipTests install` (the last module merges every module's classes into target/classes;
# TracingExample needs nothing else: the OTLP export is the framework's own, no OpenTelemetry jars):
#   docker build -t hydrangea-tracing-example:local -f k8s/tracing/Dockerfile .
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY target/classes /app/classes
ENTRYPOINT ["java", "-Xms128m", "-Xmx256m", "-cp", "/app/classes", "io.hydrangea.examples.TracingExample"]
CMD ["8080"]
