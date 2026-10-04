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
