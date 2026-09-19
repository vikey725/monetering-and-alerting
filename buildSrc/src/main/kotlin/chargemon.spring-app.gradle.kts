import org.gradle.accessors.dm.LibrariesForLibs

plugins {
    id("chargemon.java-library")
    id("org.springframework.boot")
    id("io.spring.dependency-management")
}

val libs = the<LibrariesForLibs>()

dependencies {
    implementation("org.springframework.boot:spring-boot-starter")
    testImplementation("org.springframework.boot:spring-boot-starter-test")
}
