plugins {
    java
}

dependencies {
    val sr = libs.versions.streamrune.get()
    implementation("org.streamrune:streamrune-core:$sr")
    implementation("org.streamrune:streamrune-crypto-api:$sr")
    implementation(libs.jackson.databind)
    implementation(libs.jakarta.validation.api)
}
