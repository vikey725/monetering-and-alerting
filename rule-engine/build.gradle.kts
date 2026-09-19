plugins {
    id("chargemon.java-library")
    `java-test-fixtures`
}

dependencies {
    api(project(":alert-model"))
    testImplementation(libs.jqwik)
    testFixturesApi(project(":alert-model"))
}
