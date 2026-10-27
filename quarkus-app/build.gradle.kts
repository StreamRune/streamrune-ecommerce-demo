plugins {
    java
    alias(libs.plugins.quarkus)
}

val sr = libs.versions.streamrune.get()

dependencies {
    implementation(platform(libs.quarkus.bom))

    implementation(project(":domain"))
    implementation(project(":commands"))
    implementation(project(":queries"))
    implementation(project(":projections"))
    implementation("org.streamrune:streamrune-quarkus:$sr")
    implementation("org.streamrune:streamrune-postgres:$sr")
    // GDPR crypto-shredding: PostgresCryptoEngine (postgres-crypto) and the CryptoShreddingModule
    // + REDACTED tombstone (crypto-api). Both are on the classpath transitively via the integration,
    // but are declared here explicitly because this app's code references them directly.
    implementation("org.streamrune:streamrune-crypto-api:$sr")
    implementation("org.streamrune:streamrune-postgres-crypto:$sr")
    implementation("org.streamrune:streamrune-runtime:$sr")
    // Transactional outbox → RabbitMQ
    implementation("org.streamrune:streamrune-rabbitmq-outbox:$sr")
    implementation(libs.rabbitmq.client)
    implementation("com.fasterxml.jackson.core:jackson-databind")

    // Quarkus REST (formerly RESTEasy Reactive)
    implementation("io.quarkus:quarkus-rest")
    implementation("io.quarkus:quarkus-rest-jackson")
    implementation("io.quarkus:quarkus-jdbc-postgresql")
    implementation("io.quarkus:quarkus-arc")
    // The framework's caching components (CachingUserRoleResolver, CachingQueryBus, CacheAware
    // projections) use Caffeine, which reflectively loads feature-encoded cache classes (e.g.
    // "SSMSW"). The quarkus-caffeine extension registers those for GraalVM native — without it the
    // native binary fails at startup with IllegalStateException: <cache-class-name>.
    implementation("io.quarkus:quarkus-caffeine")
    implementation(libs.reactor.core)

    // Health and metrics
    implementation("io.quarkus:quarkus-smallrye-health")
    implementation("io.quarkus:quarkus-micrometer-registry-prometheus")

    // In-memory stores (InMemoryProjectionRepository, InMemoryEventStore) for Docker-free tests.
    testImplementation("org.streamrune:streamrune-test:$sr")

    // End-to-end GDPR test: @QuarkusTest + RestAssured against a real Postgres (Testcontainers)
    // wired in via a QuarkusTestResourceLifecycleManager. Awaitility polls projection propagation.
    testImplementation("io.quarkus:quarkus-junit5")
    testImplementation("io.rest-assured:rest-assured")
    testImplementation(libs.testcontainers.postgresql)
    testImplementation(libs.testcontainers.rabbitmq)
    testImplementation(libs.testcontainers.junit)
    testImplementation(libs.awaitility)
    testImplementation(libs.assertj.core)
}

tasks.named<Test>("test") {
    // Quarkus uses preview APIs (ScopedValue) at runtime; tests run plain JVM.
    jvmArgs("--enable-preview")
}
