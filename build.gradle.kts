plugins {
    java
    alias(libs.plugins.spotless) apply false
    alias(libs.plugins.graalvm) apply false
}

subprojects {
    apply(plugin = "java-library")
    apply(plugin = "com.diffplug.spotless")

    group = "org.streamrune.ecommerce"
    version = "0.1.0-SNAPSHOT"

    java {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(25))
        }
    }

    tasks.withType<JavaCompile> {
        options.compilerArgs.addAll(listOf("--enable-preview"))
    }

    tasks.withType<Test> {
        useJUnitPlatform()
        jvmArgs("--enable-preview")
    }

    repositories {
        // Opt-in only (./gradlew -PuseMavenLocal=true ...): a stale ~/.m2 copy of the framework must
        // never shadow the published artifacts by accident.
        if (providers.gradleProperty("useMavenLocal").isPresent) {
            mavenLocal()
        }
        mavenCentral()
        // StreamRune 1.0.0-alpha-SNAPSHOT is an unreleased preview published only to the Central
        // snapshot repository; it serves the org.streamrune snapshots and nothing else. When
        // ../streamrune exists, the composite build in settings.gradle.kts replaces these
        // artifacts with the local framework source.
        maven("https://central.sonatype.com/repository/maven-snapshots/") {
            mavenContent { snapshotsOnly() }
            content { includeGroup("org.streamrune") }
        }
    }

    dependencies {
        val libs = rootProject.the<VersionCatalogsExtension>().named("libs")
        testImplementation(platform(libs.findLibrary("junit-bom").get()))
        testImplementation("org.junit.jupiter:junit-jupiter")
        testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    }

    // `check` runs spotlessCheck (Spotless's default), so `build` fails on a formatting violation.
    configure<com.diffplug.gradle.spotless.SpotlessExtension> {
        java {
            googleJavaFormat("1.34.1")
            removeUnusedImports()
        }
    }
}

// ── quickCheck — the fast lane ───────────────────────────────────────────────────────────────
//
// Compiles the main and test sources of every module, runs spotlessCheck, the tests of the shared
// modules whose tests need no Docker and spring-app's docsTest (the tutorial tests and the
// public-text ratchet) and, when frontend/node_modules exists, type-checks and lints the frontend.
// The suites of the three apps and the notifications service start Testcontainers and are left to
// `build`. A green quickCheck is a fast signal, not a substitute for `./gradlew build`.
val dockerFreeModules = listOf(":domain", ":commands", ":queries", ":projections")
val frontendDir = file("frontend")
val frontendDependencies = file("frontend/node_modules")
val npm = if (System.getProperty("os.name").startsWith("Windows")) "npm.cmd" else "npm"

val frontendChecks =
    listOf("typecheck" to "Type-checks", "lint" to "Lints").map { (script, verb) ->
        tasks.register<Exec>("frontend${script.replaceFirstChar { it.uppercase() }}") {
            group = "verification"
            description =
                "$verb the frontend (npm run $script); skipped without frontend/node_modules, " +
                    "needs npm on the PATH."
            workingDir = frontendDir
            commandLine(npm, "run", "-s", script)
            val installed = frontendDependencies
            onlyIf("frontend/node_modules exists") { installed.exists() }
            // Exec resolves the command on the PATH Gradle runs with, which can lack npm (a daemon
            // started from an IDE): say so instead of failing to start an unnamed process.
            val command = npm
            doFirst {
                val onPath =
                    System.getenv("PATH").orEmpty().split(File.pathSeparator)
                        .any { File(it, command).canExecute() }
                if (!onPath) {
                    throw GradleException(
                        "frontend/node_modules exists but `$command` is not on the PATH of this " +
                            "Gradle build: install Node.js, or start Gradle from a shell where " +
                            "`$command --version` works.")
                }
            }
        }
    }

tasks.register("quickCheck") {
    group = "verification"
    description =
        "Compiles every module, runs spotlessCheck, the tests of domain, commands, queries and " +
            "projections and the docs tests, and type-checks and lints the frontend when " +
            "frontend/node_modules exists."
    dependsOn(
        subprojects.map { "${it.path}:compileJava" },
        subprojects.map { "${it.path}:compileTestJava" })
    dependsOn(subprojects.map { "${it.path}:spotlessCheck" })
    dependsOn(dockerFreeModules.map { "$it:test" })
    dependsOn(":spring-app:docsTest")
    dependsOn(frontendChecks)
    val installed = frontendDependencies
    doLast {
        if (!installed.exists()) {
            logger.lifecycle(
                "quickCheck: the frontend was not checked (no frontend/node_modules; " +
                    "run `npm ci` in frontend/ to include it)")
        }
    }
}
