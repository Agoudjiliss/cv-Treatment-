FROM maven:3.9.8-eclipse-temurin-21 AS build
WORKDIR /build
COPY pom.xml .
COPY src ./src
RUN mvn -B clean package -DskipTests

FROM eclipse-temurin:21-jre
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /build/target/cv-gateway-0.0.1-SNAPSHOT.jar app.jar
COPY scripts/wait-for-worker.sh /wait-for-worker.sh
RUN chmod +x /wait-for-worker.sh
EXPOSE 8080
ENTRYPOINT ["/wait-for-worker.sh"]
