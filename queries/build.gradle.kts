plugins {
    java
}

dependencies {
    val sr = libs.versions.streamrune.get()
    implementation(project(":domain"))
    implementation("org.streamrune:streamrune-core:$sr")

    testImplementation("org.streamrune:streamrune-test:$sr")
}
