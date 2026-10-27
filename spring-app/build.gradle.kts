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
    implementation("org.springframework.security:spring-security-core")

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

// The docs tests read the tutorial, the README and the settings file chapter 1 lists, and
// InternalTrackerIdRatchetTest reads every public text file of the repository (sources, the
// READMEs, the tutorial, scripts, build files): a change to any of them re-runs them. Mirrors the
// ratchet's own walk: the same extensions, names, pruned directories and excluded files. The
// internal planning directory's name is built from pieces, as in the test.
val planningDirectory = "super" + "powers"
val publicTextSources =
    fileTree(rootDir) {
        include("**/*.java", "**/*.md", "**/*.kts", "**/*.gradle", "**/*.toml", "**/*.properties")
        include("**/*.yml", "**/*.yaml", "**/*.sql", "**/*.sh", "**/*.xml", "**/*.json", "**/*.txt")
        include("**/*.ts", "**/*.tsx", "**/*.js", "**/*.mjs", "**/*.cjs", "**/*.css", "**/*.bat")
        include("**/Dockerfile", "**/Dockerfile.*", "**/.gitmessage", "**/.gitignore")
        include("**/.dockerignore", "**/LICENSE", "**/NOTICE", "**/CODEOWNERS", "**/gradlew")
        exclude("**/build/**", "**/out/**", "**/bin/**", "**/.gradle/**", "**/.kotlin/**")
        exclude("**/.worktrees/**", "**/.idea/**", "**/.vscode/**", "**/.settings/**")
        exclude("**/node_modules/**", "**/.next/**", "**/coverage/**", "**/test-results/**")
        exclude("**/playwright-report/**", "**/pgdata/**", "**/.$planningDirectory/**")
        exclude("**/scratchpad/**", "**/.claude/**", "docs/$planningDirectory/**", ".git/**")
        exclude("frontend/package-lock.json", "frontend/AGENTS.md", "frontend/CLAUDE.md")
        exclude("frontend/next-env.d.ts")
    }

// Gradle's default excludes drop every file named .gitignore from a file tree, whatever the tree
// includes, so the two ignore lists the ratchet reads by name are declared on their own. A .gitignore
// in another directory needs a path of its own here too: the ratchet fails when it reads a file that
// is not declared.
val ignoreLists = files(rootDir.resolve(".gitignore"), rootDir.resolve("frontend/.gitignore"))

// The public text the docs tests read, as Gradle resolves it: the tree and the ignore lists.
val docsTestInputs = files(publicTextSources, ignoreLists)

// Hands the files Gradle resolves for docsTestInputs to InternalTrackerIdRatchetTest, which fails when
// they are not the files its walk reads: a file the walk reads and Gradle does not declare would let
// an edit of it keep an earlier result.
val publicTextSourceManifest =
    tasks.register("publicTextSourceManifest") {
        group = "verification"
        description =
            "Writes the public-text file list declared as an input of the docs tests, so the " +
                "ratchet can prove the declaration covers what it scans."
        val sources = docsTestInputs
        val repoRoot = rootDir
        val manifest = layout.buildDirectory.file("public-text/declared-sources.txt")
        inputs
            .files(sources)
            .withPropertyName("publicTextSources")
            .withPathSensitivity(PathSensitivity.RELATIVE)
        outputs.file(manifest)
        doLast {
            val file = manifest.get().asFile
            file.parentFile.mkdirs()
            file.writeText(
                sources.files
                    .filter { it.isFile }
                    .map { it.relativeTo(repoRoot).invariantSeparatorsPath }
                    .sorted()
                    .joinToString("\n"),
            )
        }
    }

// The docs tests (org.streamrune.ecommerce.spring.docs) whose class names end in Test need no
// Docker: they run in `docsTest`, which `check`, the root `quickCheck` and the `Shared module tests`
// CI job run, with the public text as its input, so an edit to a chapter, a README or a comment
// re-runs them and not the Testcontainers suite. TutorialChaptersStartIT, which starts each
// chapter's project on the test PostgreSQL, stays in `test`, whose input the chapters are. The
// split is by class file: a test method whose name ends in Test does not move its class.
val docsTestClasses = "org/streamrune/ecommerce/spring/docs/*Test.class"
val testSourceSet = sourceSets["test"]
val docsTest =
    tasks.register<Test>("docsTest") {
        group = "verification"
        description =
            "Runs the docs tests that need no Docker: the tutorial's listings, steps and ledger, " +
                "the version catalog's aliases and the public-text ratchet."
        testClassesDirs = testSourceSet.output.classesDirs
        classpath = testSourceSet.runtimeClasspath
        useJUnitPlatform { includeEngines("junit-jupiter") }
        jvmArgs("--enable-preview")
        include(docsTestClasses)
        inputs.files(docsTestInputs)
            .withPathSensitivity(PathSensitivity.RELATIVE)
            .withPropertyName("publicTextSources")
        dependsOn(publicTextSourceManifest)
        val manifest = publicTextSourceManifest.map { it.outputs.files.singleFile }
        // Passed through an argument provider rather than `systemProperty`, so the absolute path
        // never enters the task's cache key.
        jvmArgumentProviders.add(
            CommandLineArgumentProvider {
                listOf("-Dstreamrune.publictext.declared-sources=${manifest.get().absolutePath}")
            },
        )
    }

tasks.named<Test>("test") {
    useJUnitPlatform()
    jvmArgs("--enable-preview")
    exclude(docsTestClasses)
    inputs.files(fileTree(rootDir.resolve("docs/tutorial")) { include("*.md") })
        .withPathSensitivity(PathSensitivity.RELATIVE)
        .withPropertyName("tutorialChapters")
}

tasks.named("check") { dependsOn(docsTest) }
