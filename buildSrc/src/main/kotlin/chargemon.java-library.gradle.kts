import org.gradle.accessors.dm.LibrariesForLibs

plugins {
    `java-library`
}

val libs = the<LibrariesForLibs>()

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(libs.versions.java.get()))
    }
    withSourcesJar()
}

dependencies {
    api(platform(libs.jackson.bom))
    implementation(libs.slf4j.api)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testRuntimeOnly(libs.junit.platform.launcher)
    testRuntimeOnly(libs.logback.classic)
}

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
    options.compilerArgs.addAll(listOf("-Xlint:all", "-Xlint:-serial", "-Xlint:-processing", "-parameters"))
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
    testLogging {
        events("failed", "skipped")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
    maxHeapSize = "1g"
}
