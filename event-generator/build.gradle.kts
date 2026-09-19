plugins {
    id("chargemon.java-library")
    id("com.gradleup.shadow")
    application
}

application {
    mainClass.set("com.chargemon.generator.GeneratorMain")
}

dependencies {
    implementation(project(":common"))
    implementation(project(":ocpp-model"))
    implementation(project(":ocpp-codec"))
    implementation(testFixtures(project(":ocpp-codec")))
    implementation(libs.kafka.clients)
    implementation(libs.picocli)
    runtimeOnly(libs.logback.classic)
}

tasks.shadowJar {
    archiveClassifier.set("all")
    archiveVersion.set("")
    mergeServiceFiles()
}
