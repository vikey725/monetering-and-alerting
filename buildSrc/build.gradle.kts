plugins {
    `kotlin-dsl`
}

repositories {
    mavenCentral()
    gradlePluginPortal()
}

dependencies {
    // Expose the generated version-catalog accessors (LibrariesForLibs) to precompiled script plugins.
    implementation(files(libs.javaClass.superclass.protectionDomain.codeSource.location))
    implementation("org.springframework.boot:org.springframework.boot.gradle.plugin:${libs.versions.springBoot.get()}")
    implementation("io.spring.dependency-management:io.spring.dependency-management.gradle.plugin:${libs.versions.springDependencyManagement.get()}")
    implementation("com.gradleup.shadow:shadow-gradle-plugin:${libs.versions.shadow.get()}")
}
