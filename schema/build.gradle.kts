plugins {
    id("chargemon.java-library")
    id("com.gradleup.shadow")
    application
}

application {
    mainClass.set("com.chargemon.schema.SchemaMigrator")
}

dependencies {
    implementation(project(":common"))
    implementation(libs.flyway.core)
    implementation(libs.flyway.postgresql)
    implementation(libs.postgresql)
    runtimeOnly(libs.logback.classic)

    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.testcontainers.junit)
    testImplementation(libs.testcontainers.postgresql)
}

tasks.shadowJar {
    archiveClassifier.set("all")
    archiveVersion.set("")
    mergeServiceFiles()
}
