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

    configure<com.diffplug.gradle.spotless.SpotlessExtension> {
        setEnforceCheck(false)
        java {
            googleJavaFormat("1.34.1")
            removeUnusedImports()
        }
    }
}
