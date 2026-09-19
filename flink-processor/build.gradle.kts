plugins {
    id("chargemon.flink-app")
}

application {
    mainClass.set("com.chargemon.flink.JobMain")
}

dependencies {
    implementation(project(":common"))
    implementation(project(":ocpp-model"))
    implementation(project(":ocpp-codec"))
    implementation(project(":alert-model"))
    implementation(project(":rule-engine"))

    "flinkProvided"(libs.flink.streaming)
    "flinkProvided"(libs.flink.clients)
    "flinkProvided"(libs.flink.runtime.web)

    implementation(libs.flink.connector.base)
    implementation(libs.flink.connector.kafka)
    implementation(libs.flink.connector.jdbc.core)
    implementation(libs.flink.connector.jdbc.postgres)
    implementation(libs.flink.statebackend.rocksdb)
    implementation(libs.flink.metrics.prometheus)
    implementation(libs.postgresql)
    implementation(libs.jackson.databind)
    implementation(libs.jackson.dataformat.smile)
    runtimeOnly(libs.logback.classic)

    testImplementation(libs.flink.test.utils)
    testImplementation(testFixtures(project(":ocpp-codec")))
    testImplementation(testFixtures(project(":rule-engine")))
    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.testcontainers.junit)
    testImplementation(libs.testcontainers.kafka)
    testImplementation(libs.testcontainers.postgresql)
}
