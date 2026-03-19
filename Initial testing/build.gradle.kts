plugins {
    java
    application
    id("com.google.protobuf") version "0.9.4"
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

dependencies {
    // Kafka
    implementation("org.apache.kafka:kafka-clients:3.7.0")

    // HDFS
    implementation("org.apache.hadoop:hadoop-client:3.4.0")

    // etcd (IdRegistry)
    implementation("io.etcd:jetcd-core:0.7.7")

    // Redis / CacheEventStore
    implementation("io.lettuce:lettuce-core:6.3.2.RELEASE")

    // HBase / LogsEventStore
    implementation("org.apache.hbase:hbase-client:2.5.9")

    // gRPC
    implementation("io.grpc:grpc-netty-shaded:1.63.0")
    implementation("io.grpc:grpc-protobuf:1.63.0")
    implementation("io.grpc:grpc-stub:1.63.0")

    // Protobuf (event schemas)
    implementation("com.google.protobuf:protobuf-java:3.25.3")

    // Prometheus metrics
    implementation("io.prometheus:simpleclient:0.16.0")
    implementation("io.prometheus:simpleclient_hotspot:0.16.0")
    implementation("io.prometheus:simpleclient_httpserver:0.16.0")

    // Retry / resilience
    implementation("io.github.resilience4j:resilience4j-retry:2.2.0")

    // Logging
    implementation("org.slf4j:slf4j-api:2.0.13")
    runtimeOnly("ch.qos.logback:logback-classic:1.5.6")

    // Annotation processor needed for gRPC generated stubs
    compileOnly("javax.annotation:javax.annotation-api:1.3.2")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:3.25.3"
    }
    plugins {
        create("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:1.63.0"
        }
    }
    generateProtoTasks {
        all().forEach {
            it.plugins {
                create("grpc")
            }
        }
    }
}

application {
    mainClass.set("com.photon.Main")
}

configurations.all {
    exclude(group = "org.mortbay.jetty")
    exclude(group = "javax.servlet", module = "servlet-api")
    resolutionStrategy.force("io.netty:netty-all:4.1.110.Final")
}