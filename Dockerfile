FROM eclipse-temurin:17-jdk

COPY build/libs/asb-debug-1.0-SNAPSHOT-all.jar /app/app.jar

ENTRYPOINT ["bash", "-c", "exec java -jar /app/app.jar"]