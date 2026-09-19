plugins {
    id("chargemon.java-library")
    `java-test-fixtures`
}

dependencies {
    api(project(":ocpp-model"))
    api(project(":rule-engine"))
    testFixturesApi(project(":ocpp-model"))
    testFixturesImplementation(libs.jackson.databind)
}
