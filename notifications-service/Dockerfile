# Pre-built JAR approach: run ./gradlew :notifications-service:bootJar locally first,
# then docker compose build copies the result. This avoids needing the
# StreamRune framework source inside the Docker build context.
FROM eclipse-temurin:25-jre
WORKDIR /app
COPY notifications-service/build/libs/notifications-service-0.1.0-SNAPSHOT.jar app.jar
EXPOSE 8090
ENTRYPOINT ["java", "--enable-preview", "-jar", "app.jar"]
