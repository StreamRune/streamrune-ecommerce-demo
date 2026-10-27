plugins {
    java
}

dependencies {
    val sr = libs.versions.streamrune.get()
    implementation(project(":domain"))
    implementation("org.streamrune:streamrune-core:$sr")
    // CommandRefusal: CommandDeadLetteredException, what the command bus throws once a failed
    // command's dead-letter entry is stored.
    implementation("org.streamrune:streamrune-runtime:$sr")
    implementation(libs.jackson.databind)
    // LazyRabbitMqOutboxPublisher, the outbox publisher the three apps wire: it takes the apps'
    // amqp-client ConnectionFactory and builds a RabbitMqOutboxPublisher on its first publish.
    implementation("org.streamrune:streamrune-rabbitmq-outbox:$sr")
    api(libs.rabbitmq.client)
    implementation(libs.slf4j.api)

    testImplementation("org.streamrune:streamrune-test:$sr")
    testImplementation(libs.assertj.core)
}
