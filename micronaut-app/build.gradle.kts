plugins {
    java
    alias(libs.plugins.spring.dependency.management)
    alias(libs.plugins.micronaut.application)
}

application {
    mainClass.set("org.streamrune.ecommerce.micronaut.MicronautEcommerceApplication")
}

// The native image is the application, not a library: without this native-build-tools 1.1.2
// generated `micronaut-app.dylib` (shared library) instead of an executable (same setting as
// spring-app).
graalvmNative {
    binaries {
        named("main") {
            sharedLibrary.set(false)
            // Netty 4.2 (Micronaut's HTTP server) frees each direct buffer it allocates outside its
            // pool through Arena.ofShared().close() on Java 25: a buffer above 1 MiB on release, and
            // every pooled chunk when an event loop exits at shutdown. GraalVM 25 supports shared
            // arenas only with -H:+SharedArenaSupport (off by default, an experimental option, hence
            // the unlock pair); without it the image throws UnsupportedFeatureError ("Support for
            // Arena.ofShared is not active") on the event loop and never frees the memory. Netty's
            // own probe cannot catch this: netty-common initializes CleanerJava25 at image build
            // time, so the probe runs on the build JVM. scripts/native-image-smoke-test.sh serves a
            // 1.5 MiB read model and fails on any UnsupportedFeatureError, shutdown included.
            buildArgs.addAll(
                "-H:+UnlockExperimentalVMOptions",
                "-H:+SharedArenaSupport",
                "-H:-UnlockExperimentalVMOptions",
            )
        }
    }
    // native-build-tools refuses its default reachability-metadata repository when the GraalVM
    // installation has no lib/svm/schemas/reachability-metadata-schema.json (GraalVM CE 25.0.1 has
    // none). -PgraalvmMetadataRepository=<dir> (or ORG_GRADLE_PROJECT_graalvmMetadataRepository)
    // points it at another copy of that repository, e.g. one without its
    // schemas/reachability-metadata-schema-v*.json — see README, "GraalVM Native Image".
    providers.gradleProperty("graalvmMetadataRepository").orNull?.let { repository ->
        metadataRepository { uri(file(repository)) }
    }
}

micronaut {
    // The version catalog exposes "micronaut = 4.10.15" (library version) which the plugin
    // picks up for its BOM import; micronaut-platform doesn't publish that version, so we
    // disable the auto-import and manage all Micronaut dependency versions via the catalog.
    importMicronautPlatform.set(false)
}

val sr = libs.versions.streamrune.get()

dependencies {
    // Annotation processors must be explicit when importMicronautPlatform is disabled
    annotationProcessor(libs.micronaut.graal)
    annotationProcessor(libs.micronaut.inject.java)
    annotationProcessor(libs.micronaut.serde.processor)
    testAnnotationProcessor(libs.micronaut.graal)
    testAnnotationProcessor(libs.micronaut.inject.java)
    testAnnotationProcessor(libs.micronaut.serde.processor)
    implementation(project(":domain"))
    implementation(project(":commands"))
    implementation(project(":queries"))
    implementation(project(":projections"))
    implementation("org.streamrune:streamrune-micronaut:$sr")
    implementation("org.streamrune:streamrune-postgres:$sr")
    implementation("org.streamrune:streamrune-runtime:$sr")
    // GDPR crypto-shredding: per-subject AES-256 keys backing the @Encrypted customer fields,
    // plus the ForgetSubjectService / SubjectDataPurger SPI used by the customer forget flow.
    implementation("org.streamrune:streamrune-crypto-api:$sr")
    implementation("org.streamrune:streamrune-postgres-crypto:$sr")
    implementation("org.streamrune:streamrune-rabbitmq-outbox:$sr")
    implementation(libs.rabbitmq.client)
    implementation("com.fasterxml.jackson.core:jackson-databind")
    // StreamRuneFactory registers JavaTimeModule on its ObjectMapper, so the module is a direct
    // dependency. It used to arrive only because flyway-core 10.22 (reached through
    // streamrune-postgres) declared it at compile scope; flyway-core 13 does not, so the app
    // must say so itself. Version: managed by the dependency-management plugin, like databind.
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")

    // Micronaut HTTP
    implementation(libs.micronaut.http.server.netty)
    implementation(libs.micronaut.inject)
    implementation(libs.micronaut.serde.jackson)
    implementation(libs.micronaut.jdbc.hikari)
    implementation(libs.micronaut.management)
    implementation(libs.jakarta.annotation.api)
    implementation(libs.snakeyaml)
    implementation(libs.reactor.core)

    // PostgreSQL JDBC driver for the Hikari datasource.
    runtimeOnly(libs.postgresql)

    // SLF4J backend — without it Micronaut startup failures are silently swallowed
    runtimeOnly("ch.qos.logback:logback-classic:1.5.18")

    // In-memory stores (InMemoryProjectionRepository, InMemoryEventStore) for Docker-free tests.
    testImplementation("org.streamrune:streamrune-test:$sr")

    // === E2E HTTP test stack: real Micronaut server on a Testcontainers PostgreSQL ===
    testImplementation(libs.micronaut.test.junit5)
    testImplementation(
        "io.micronaut:micronaut-http-client:${libs.versions.micronaut.asProvider().get()}")
    testAnnotationProcessor(libs.micronaut.inject.java)
    testImplementation(libs.testcontainers.postgresql)
    testImplementation(libs.testcontainers.junit)
    testImplementation(libs.testcontainers.rabbitmq)
    testImplementation(libs.awaitility)
    testImplementation(libs.assertj.core)
    testImplementation(libs.postgresql)
}
