plugins {
    id("chargemon.spring-app")
}

dependencies {
    implementation(project(":common"))
    implementation(project(":alert-model"))

    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-jdbc")
    implementation("org.springframework.boot:spring-boot-starter-mail")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    implementation("org.springframework.kafka:spring-kafka")
    implementation("io.micrometer:micrometer-registry-prometheus")
    runtimeOnly(libs.postgresql)

    testImplementation(project(":schema"))
    testImplementation(libs.postgresql)
    testImplementation(libs.flyway.core)
    testImplementation(libs.flyway.postgresql)
    testImplementation("org.springframework.kafka:spring-kafka-test")
    testImplementation(libs.wiremock)
    testImplementation(libs.greenmail)
    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.testcontainers.junit)
    testImplementation(libs.testcontainers.postgresql)
}

tasks.bootJar {
    archiveFileName.set("notifier.jar")
}
