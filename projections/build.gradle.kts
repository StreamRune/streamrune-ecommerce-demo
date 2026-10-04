plugins {
    java
}

dependencies {
    val sr = libs.versions.streamrune.get()
    implementation(project(":domain"))
    implementation(project(":queries"))
    implementation("org.streamrune:streamrune-core:$sr")
    implementation(libs.jackson.databind)

    testImplementation(libs.assertj.core)
    testImplementation("org.streamrune:streamrune-test:$sr")
    testImplementation("org.streamrune:streamrune-runtime:$sr")
}
