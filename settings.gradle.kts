rootProject.name = "streamrune-ecommerce-demo"

// Composite build: resolve StreamRune from local source when available.
// Falls back to the published StreamRune artifacts when the directory doesn't exist
// (e.g., inside a Docker build or CI without the framework checkout). The default clone
// directory of github.com/StreamRune/streamrune is lowercase, so the name matters on
// case-sensitive file systems.
val frameworkDir = file("../streamrune")
if (frameworkDir.isDirectory) {
    includeBuild(frameworkDir) {
        dependencySubstitution {
            substitute(module("org.streamrune:streamrune-core")).using(project(":streamrune-core"))
            substitute(module("org.streamrune:streamrune-runtime")).using(project(":streamrune-runtime"))
            substitute(module("org.streamrune:streamrune-test")).using(project(":streamrune-test"))
            substitute(module("org.streamrune:streamrune-crypto-api")).using(project(":streamrune-crypto:streamrune-crypto-api"))
            substitute(module("org.streamrune:streamrune-postgres-crypto")).using(project(":streamrune-crypto:streamrune-postgres-crypto"))
            substitute(module("org.streamrune:streamrune-postgres")).using(project(":streamrune-eventstore:streamrune-postgres"))
            substitute(module("org.streamrune:streamrune-spring")).using(project(":streamrune-integration:streamrune-spring"))
            substitute(module("org.streamrune:streamrune-quarkus")).using(project(":streamrune-integration:streamrune-quarkus"))
            substitute(module("org.streamrune:streamrune-micronaut")).using(project(":streamrune-integration:streamrune-micronaut"))
            substitute(module("org.streamrune:streamrune-rabbitmq-outbox")).using(project(":streamrune-outbox:streamrune-rabbitmq-outbox"))
        }
    }
}

// Every build says which framework it compiles against: the sibling checkout and its revision, or
// the published artifacts (a warning). -PrequireFrameworkSibling turns the second case into a
// failure, for a run that must test the local framework source rather than a published build.
val frameworkVersion: String? =
    file("gradle/libs.versions.toml").readLines()
        .firstNotNullOfOrNull { Regex("""^streamrune\s*=\s*"([^"]+)"""").find(it)?.groupValues?.get(1) }
if (frameworkDir.isDirectory) {
    val revision =
        runCatching {
            providers.exec {
                commandLine("git", "-C", frameworkDir.path, "rev-parse", "--short", "HEAD")
                isIgnoreExitValue = true
            }.standardOutput.asText.get().trim()
        }.getOrNull()
    logger.lifecycle(
        "StreamRune framework: composite ../streamrune @ ${revision?.ifEmpty { null } ?: "unknown revision"}")
} else {
    val published =
        when {
            frameworkVersion == null -> "the published artifacts"
            frameworkVersion.endsWith("-SNAPSHOT") -> "Central snapshot $frameworkVersion"
            else -> "Maven Central $frameworkVersion"
        }
    val message = "StreamRune framework: $published (no ../streamrune sibling)"
    if (providers.gradleProperty("requireFrameworkSibling").map { it != "false" }.getOrElse(false)) {
        throw GradleException(
            "$message. -PrequireFrameworkSibling needs a framework checkout in ../streamrune: " +
                "git clone https://github.com/StreamRune/streamrune.git ../streamrune")
    }
    logger.warn(message)
}

include(
    "domain",
    "commands",
    "queries",
    "projections",
    "spring-app",
    "quarkus-app",
    "micronaut-app",
    "notifications-service"
)
