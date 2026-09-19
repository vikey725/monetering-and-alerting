import org.gradle.accessors.dm.LibrariesForLibs

plugins {
    id("chargemon.java-library")
    id("com.gradleup.shadow")
    application
}

val libs = the<LibrariesForLibs>()

// Flink runtime jars are provided by the cluster; keep them off the fat jar.
val flinkProvided: Configuration by configurations.creating
configurations.compileOnly.get().extendsFrom(flinkProvided)
configurations.testImplementation.get().extendsFrom(flinkProvided)

// Flink 1.20.5 depends on the at.yawk.lz4 fork, which declares the org.lz4:lz4-java capability
// that kafka-clients also brings in. Pick the newer one.
configurations.all {
    resolutionStrategy.capabilitiesResolution.withCapability("org.lz4:lz4-java") {
        selectHighestVersion()
    }
}

tasks.shadowJar {
    archiveClassifier.set("all")
    archiveVersion.set("")
    mergeServiceFiles()
    isZip64 = true
    manifest {
        attributes["Main-Class"] = application.mainClass.get()
    }
    // Flink's own jackson is shaded; ours must stay unshaded and is bundled.
}

tasks.named("build") {
    dependsOn(tasks.shadowJar)
}
