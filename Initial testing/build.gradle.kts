plugins {
    java
    application
    id("com.google.protobuf") version "0.9.4"   // ← add this
}

group = "com.photon"
version = "1.0.0"

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

repositories {
    mavenCentral()
}

val grpcVersion     = "1.63.0"
val protobufVersion = "3.25.3"

dependencies {
    implementation("org.apache.kafka:kafka-clients:3.7.0")
    implementation("org.apache.hadoop:hadoop-client:3.4.0")
    implementation("io.etcd:jetcd-core:0.7.7")
    implementation("io.lettuce:lettuce-core:6.3.2.RELEASE")
    implementation("org.apache.hbase:hbase-client:2.5.9")
    implementation("io.grpc:grpc-netty-shaded:1.63.0")
    implementation("io.grpc:grpc-protobuf:1.63.0")
    implementation("io.grpc:grpc-stub:1.63.0")
    implementation("com.google.protobuf:protobuf-java:3.25.3")
    implementation("io.prometheus:simpleclient:0.16.0")
    implementation("io.prometheus:simpleclient_hotspot:0.16.0")
    implementation("io.prometheus:simpleclient_httpserver:0.16.0")
    implementation("io.github.resilience4j:resilience4j-retry:2.2.0")
    implementation("org.slf4j:slf4j-api:2.0.13")
    runtimeOnly("ch.qos.logback:logback-classic:1.5.6")
    compileOnly("javax.annotation:javax.annotation-api:1.3.2")

    implementation("org.apache.hadoop:hadoop-client:3.4.0")
    implementation("org.apache.hbase:hbase-client:2.5.9")

    testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.10.2")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.10.2")
    testImplementation("org.testcontainers:testcontainers:1.19.8")
    testImplementation("org.testcontainers:junit-jupiter:1.19.8")


}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:$protobufVersion"
    }
    plugins {
        create("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:$grpcVersion"
        }
    }
    generateProtoTasks {
        all().forEach { task ->
            task.plugins {
                create("grpc")
            }
        }
    }
}

sourceSets {
    main {
        java {
            srcDir("build/generated/source/proto/main/grpc")
            srcDir("build/generated/source/proto/main/java")
        }
    }
}

application {
    mainClass.set("com.photon.Main")
}

tasks.test {
    useJUnitPlatform()
}

configurations.all {
    exclude(group = "org.mortbay.jetty")
    exclude(group = "javax.servlet", module = "servlet-api")
    resolutionStrategy.force("io.netty:netty-all:4.1.110.Final")
}