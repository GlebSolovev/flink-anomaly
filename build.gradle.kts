plugins {
    java
    application
}

group = "ru.hse.flinkanomaly"
version = "1.0"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.flink:flink-streaming-java_2.12:1.14.4")
    implementation("org.apache.flink:flink-core:1.14.4") { // unused for now
        exclude(group = "commons-logging", module = "commons-logging") // exclude to use local log4j.properties
    }
    implementation("org.apache.flink:flink-java:1.14.4") {
        exclude(group = "commons-logging", module = "commons-logging") // exclude to use local log4j.properties
    }
    implementation("org.apache.flink:flink-clients_2.12:1.14.4") // must have to execute program

    // flink logging
    implementation("org.slf4j:slf4j-api:1.7.36")
    implementation("org.slf4j:slf4j-log4j12:1.7.36")
}

java {
    sourceSets {
        main {
            java.setSrcDirs(listOf("src/main"))
            resources.setSrcDirs(listOf("src/resources"))
        }
        test {
            java.setSrcDirs(listOf("src/test"))
        }
    }
}
