plugins {
    java
    alias(libs.plugins.spring.dependency.management)
    alias(libs.plugins.spring.boot)
    alias(libs.plugins.graalvm)
}

graalvmNative {
    binaries {
        named("main") {
            sharedLibrary.set(false)
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

// NOTE: do NOT disable the `jar` task. Spring Boot 4's AOT/native integration puts
// this module's own classes on the native-image classpath via the plain `-plain.jar`
// artifact (the `jar` task output), not the exploded build/classes/java/main dir.
// Disabling `jar` leaves that classpath entry pointing at a non-existent file, so
// nativeCompile fails with "Main entry point class ... neither found on classpath".

configure<com.diffplug.gradle.spotless.SpotlessExtension> {
    java {
        target("src/**/*.java")
        targetExclude("build/generated/**")
    }
}

tasks.named("collectReachabilityMetadata") {
    onlyIf { gradle.startParameter.taskNames.any { it.contains("native") } }
}

// processAot runs the application context to generate AOT sources — it requires
// a live database.  Skip it during regular builds; it is only needed when
// building a GraalVM native image.
tasks.named<JavaExec>("processAot") {
    onlyIf { gradle.startParameter.taskNames.any { it.contains("native") } }
    jvmArgs("--enable-preview")
    systemProperty("spring.datasource.url", "jdbc:postgresql://localhost:5432/streamrune_ecommerce")
    systemProperty("spring.datasource.driver-class-name", "org.postgresql.Driver")
    systemProperty("spring.datasource.username", "postgres")
    systemProperty("spring.datasource.password", "postgres")
    systemProperty("spring.sql.init.mode", "never")
}

// processTestAot would similarly need a live database to spin up the test context.
// Tests should run plain JVM, not AOT — disable unless we explicitly target native tests.
tasks.named<JavaExec>("processTestAot") {
    onlyIf { gradle.startParameter.taskNames.any { it.contains("native") } }
}

val sr = libs.versions.streamrune.get()

dependencies {
    implementation(project(":domain"))
    implementation(project(":commands"))
    implementation(project(":queries"))
    implementation(project(":projections"))
    implementation("org.streamrune:streamrune-spring:$sr")
    implementation("org.streamrune:streamrune-postgres:$sr")
    implementation("com.fasterxml.jackson.core:jackson-databind")
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-webflux")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    implementation("org.springframework.boot:spring-boot-starter-jdbc")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.postgresql:postgresql")
    implementation("org.streamrune:streamrune-crypto-api:$sr")
    implementation("org.streamrune:streamrune-postgres-crypto:$sr")
    implementation("org.streamrune:streamrune-runtime:$sr")
    implementation("org.streamrune:streamrune-rabbitmq-outbox:$sr")
    implementation("org.springframework.boot:spring-boot-starter-amqp")
    // Required for AOT: StreamRuneAutoConfiguration references these optional classes.
    implementation(libs.opentelemetry.api)
    implementation(libs.spring.security.core)

    testImplementation(libs.spring.boot.starter.test)
    // The tutorial listings compiled by TutorialListingsCompileTest include decider tests.
    testImplementation("org.streamrune:streamrune-test:$sr")
    testImplementation(libs.testcontainers.postgresql)
    testImplementation(libs.testcontainers.rabbitmq)
    testImplementation(libs.testcontainers.junit)
    testImplementation(libs.awaitility)
    testImplementation(libs.assertj.core)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.named<Test>("test") {
    useJUnitPlatform()
    jvmArgs("--enable-preview")
    // The docs tests read the tutorial and the README: a changed page re-runs them.
    inputs.dir(rootProject.layout.projectDirectory.dir("docs/tutorial"))
        .withPathSensitivity(PathSensitivity.RELATIVE)
        .withPropertyName("tutorial")
    inputs.file(rootProject.layout.projectDirectory.file("README.md"))
        .withPathSensitivity(PathSensitivity.RELATIVE)
        .withPropertyName("readme")
}
