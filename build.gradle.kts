import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import com.google.protobuf.gradle.*

plugins {
    kotlin("jvm") version "1.4.31"
    id("com.google.protobuf") version "0.8.15"
    application
}

group = "io.ys"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
    jcenter()
}

dependencies {
    testImplementation(kotlin("test-junit5"))
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.6.0")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.6.0")
    testImplementation("org.testcontainers:testcontainers:1.15.2")
    testImplementation("org.testcontainers:postgresql:1.15.2")
    testImplementation("io.kotest:kotest-extensions-testcontainers:4.4.1")
    testImplementation("io.kotest:kotest-runner-junit5-jvm:4.4.1")
    testImplementation("io.kotest:kotest-assertions-core-jvm:4.4.1")

    implementation("io.projectreactor:reactor-core:3.4.3")
    implementation("io.projectreactor.kafka:reactor-kafka:1.3.2")
    implementation("io.projectreactor.kotlin:reactor-kotlin-extensions:1.1.2")

    implementation("io.github.microutils:kotlin-logging:2.0.6")
    implementation("ch.qos.logback:logback-classic:1.2.3")

    implementation("com.google.protobuf:protobuf-java:3.14.0")
    implementation("com.google.protobuf:protoc:3.14.0")
    implementation("com.google.protobuf:protobuf-java-util:3.14.0")
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<KotlinCompile>() {
    kotlinOptions.jvmTarget = "11"
}

application {
    mainClassName = "MainKt"
}

protobuf {
    protoc {
        // The artifact spec for the Protobuf Compiler
        artifact = "com.google.protobuf:protoc:3.14.0"
    }
    generatedFilesBaseDir = "src/generated"
    plugins {
        // Optional: an artifact spec for a protoc plugin, with "grpc" as
        // the identifier, which can be referred to in the "plugins"
        // container of the "generateProtoTasks" closure.
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:1.16.1"
        }
    }
    generateProtoTasks {
        ofSourceSet("main").forEach {
            it.plugins {
                // Apply the "grpc" plugin whose spec is defined above, without options.
                id("grpc")
            }
        }
    }
}

sourceSets {
    main {
        java.srcDirs("src/generated/main/java")
    }
}