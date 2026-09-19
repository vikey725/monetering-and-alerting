plugins {
    id("chargemon.java-library")
}

dependencies {
    api(libs.jackson.databind)
    api(libs.jackson.datatype.jsr310)
    api(libs.jackson.datatype.jdk8)
    api(libs.jackson.dataformat.smile)
}
