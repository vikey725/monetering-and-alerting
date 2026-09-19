pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

dependencyResolutionManagement {
    repositories {
        mavenCentral()
    }
}

rootProject.name = "chargemon"

include(
    "common",
    "ocpp-model",
    "ocpp-codec",
    "alert-model",
    "rule-engine",
    "schema",
    "flink-processor",
    "notifier",
    "event-generator",
)
