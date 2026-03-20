@rem
@rem Copyright 2015 the original author or authors.
@rem
@rem Licensed under the Apache License, Version 2.0 (the "License");
@rem you may not use this file except in compliance with the License.
@rem You may obtain a copy of the License at
@rem
@rem      https://www.apache.org/licenses/LICENSE-2.0
@rem
@rem Unless required by applicable law or agreed to in writing, software
@rem distributed under the License is distributed on an "AS IS" BASIS,
@rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@rem See the License for the specific language governing permissions and
@rem limitations under the License.
@rem
@rem SPDX-License-Identifier: Apache-2.0
@rem

@if "%DEBUG%"=="" @echo off
@rem ##########################################################################
@rem
@rem  Initial testing startup script for Windows
@rem
@rem ##########################################################################

@rem Set local scope for the variables with windows NT shell
if "%OS%"=="Windows_NT" setlocal

set DIRNAME=%~dp0
if "%DIRNAME%"=="" set DIRNAME=.
@rem This is normally unused
set APP_BASE_NAME=%~n0
set APP_HOME=%DIRNAME%..

@rem Resolve any "." and ".." in APP_HOME to make it shorter.
for %%i in ("%APP_HOME%") do set APP_HOME=%%~fi

@rem Add default JVM options here. You can also use JAVA_OPTS and INITIAL_TESTING_OPTS to pass JVM options to this script.
set DEFAULT_JVM_OPTS=

@rem Find java.exe
if defined JAVA_HOME goto findJavaFromJavaHome

set JAVA_EXE=java.exe
%JAVA_EXE% -version >NUL 2>&1
if %ERRORLEVEL% equ 0 goto execute

echo. 1>&2
echo ERROR: JAVA_HOME is not set and no 'java' command could be found in your PATH. 1>&2
echo. 1>&2
echo Please set the JAVA_HOME variable in your environment to match the 1>&2
echo location of your Java installation. 1>&2

goto fail

:findJavaFromJavaHome
set JAVA_HOME=%JAVA_HOME:"=%
set JAVA_EXE=%JAVA_HOME%/bin/java.exe

if exist "%JAVA_EXE%" goto execute

echo. 1>&2
echo ERROR: JAVA_HOME is set to an invalid directory: %JAVA_HOME% 1>&2
echo. 1>&2
echo Please set the JAVA_HOME variable in your environment to match the 1>&2
echo location of your Java installation. 1>&2

goto fail

:execute
@rem Setup the command line

set CLASSPATH=%APP_HOME%\lib\Initial testing-1.0.0.jar;%APP_HOME%\lib\kafka-clients-3.7.0.jar;%APP_HOME%\lib\hadoop-client-3.4.0.jar;%APP_HOME%\lib\jetcd-core-0.7.7.jar;%APP_HOME%\lib\lettuce-core-6.3.2.RELEASE.jar;%APP_HOME%\lib\hbase-client-2.5.9.jar;%APP_HOME%\lib\grpc-netty-shaded-1.63.0.jar;%APP_HOME%\lib\jetcd-grpc-0.7.7.jar;%APP_HOME%\lib\vertx-grpc-4.5.1.jar;%APP_HOME%\lib\grpc-grpclb-1.60.0.jar;%APP_HOME%\lib\grpc-protobuf-1.63.0.jar;%APP_HOME%\lib\grpc-stub-1.63.0.jar;%APP_HOME%\lib\hadoop-mapreduce-client-jobclient-3.4.0.jar;%APP_HOME%\lib\hadoop-mapreduce-client-common-3.4.0.jar;%APP_HOME%\lib\hadoop-mapreduce-client-core-3.4.0.jar;%APP_HOME%\lib\hadoop-yarn-client-3.4.0.jar;%APP_HOME%\lib\hadoop-yarn-common-3.4.0.jar;%APP_HOME%\lib\hadoop-yarn-api-3.4.0.jar;%APP_HOME%\lib\hbase-hadoop2-compat-2.5.9.jar;%APP_HOME%\lib\hbase-hadoop-compat-2.5.9.jar;%APP_HOME%\lib\hbase-metrics-2.5.9.jar;%APP_HOME%\lib\hbase-metrics-api-2.5.9.jar;%APP_HOME%\lib\hbase-common-2.5.9.jar;%APP_HOME%\lib\hbase-protocol-2.5.9.jar;%APP_HOME%\lib\proto-google-common-protos-2.29.0.jar;%APP_HOME%\lib\protobuf-java-util-3.24.0.jar;%APP_HOME%\lib\protobuf-java-3.25.3.jar;%APP_HOME%\lib\simpleclient_hotspot-0.16.0.jar;%APP_HOME%\lib\simpleclient_httpserver-0.16.0.jar;%APP_HOME%\lib\simpleclient_common-0.16.0.jar;%APP_HOME%\lib\simpleclient-0.16.0.jar;%APP_HOME%\lib\resilience4j-retry-2.2.0.jar;%APP_HOME%\lib\hadoop-common-3.4.0.jar;%APP_HOME%\lib\hadoop-auth-3.4.0.jar;%APP_HOME%\lib\zookeeper-3.8.4.jar;%APP_HOME%\lib\logback-classic-1.5.6.jar;%APP_HOME%\lib\jetcd-api-0.7.7.jar;%APP_HOME%\lib\jetcd-common-0.7.7.jar;%APP_HOME%\lib\metrics-core-3.2.6.jar;%APP_HOME%\lib\resilience4j-core-2.2.0.jar;%APP_HOME%\lib\slf4j-reload4j-1.7.36.jar;%APP_HOME%\lib\avro-1.9.2.jar;%APP_HOME%\lib\dnsjava-3.4.0.jar;%APP_HOME%\lib\hbase-logging-2.5.9.jar;%APP_HOME%\lib\hbase-unsafe-4.1.5.jar;%APP_HOME%\lib\kerb-simplekdc-2.0.3.jar;%APP_HOME%\lib\kerb-client-2.0.3.jar;%APP_HOME%\lib\kerb-admin-2.0.3.jar;%APP_HOME%\lib\kerb-util-2.0.3.jar;%APP_HOME%\lib\token-provider-2.0.3.jar;%APP_HOME%\lib\kerb-server-2.0.3.jar;%APP_HOME%\lib\kerb-identity-2.0.3.jar;%APP_HOME%\lib\kerb-common-2.0.3.jar;%APP_HOME%\lib\kerb-crypto-2.0.3.jar;%APP_HOME%\lib\kerb-core-2.0.3.jar;%APP_HOME%\lib\kerby-pkix-2.0.3.jar;%APP_HOME%\lib\kerby-config-2.0.3.jar;%APP_HOME%\lib\slf4j-api-2.0.13.jar;%APP_HOME%\lib\grpc-util-1.63.0.jar;%APP_HOME%\lib\grpc-netty-1.60.0.jar;%APP_HOME%\lib\grpc-core-1.63.0.jar;%APP_HOME%\lib\grpc-protobuf-lite-1.63.0.jar;%APP_HOME%\lib\grpc-context-1.63.0.jar;%APP_HOME%\lib\grpc-api-1.63.0.jar;%APP_HOME%\lib\guice-4.2.3.jar;%APP_HOME%\lib\guava-33.2.0-jre.jar;%APP_HOME%\lib\zstd-jni-1.5.5-6.jar;%APP_HOME%\lib\lz4-java-1.8.0.jar;%APP_HOME%\lib\snappy-java-1.1.10.5.jar;%APP_HOME%\lib\hadoop-hdfs-client-3.4.0.jar;%APP_HOME%\lib\hadoop-annotations-3.4.0.jar;%APP_HOME%\lib\failsafe-3.3.2.jar;%APP_HOME%\lib\netty-all-4.1.110.Final.jar;%APP_HOME%\lib\vertx-core-4.5.1.jar;%APP_HOME%\lib\netty-codec-http2-4.1.110.Final.jar;%APP_HOME%\lib\netty-handler-proxy-4.1.110.Final.jar;%APP_HOME%\lib\netty-codec-http-4.1.110.Final.jar;%APP_HOME%\lib\netty-resolver-dns-native-macos-4.1.110.Final-osx-x86_64.jar;%APP_HOME%\lib\netty-resolver-dns-native-macos-4.1.110.Final-osx-aarch_64.jar;%APP_HOME%\lib\netty-resolver-dns-classes-macos-4.1.110.Final.jar;%APP_HOME%\lib\netty-resolver-dns-4.1.110.Final.jar;%APP_HOME%\lib\netty-handler-4.1.110.Final.jar;%APP_HOME%\lib\netty-transport-native-epoll-4.1.110.Final.jar;%APP_HOME%\lib\netty-transport-native-epoll-4.1.110.Final-linux-x86_64.jar;%APP_HOME%\lib\netty-transport-native-epoll-4.1.110.Final-linux-aarch_64.jar;%APP_HOME%\lib\netty-transport-native-epoll-4.1.110.Final-linux-riscv64.jar;%APP_HOME%\lib\netty-transport-classes-epoll-4.1.110.Final.jar;%APP_HOME%\lib\netty-transport-native-kqueue-4.1.110.Final-osx-x86_64.jar;%APP_HOME%\lib\netty-transport-native-kqueue-4.1.110.Final-osx-aarch_64.jar;%APP_HOME%\lib\netty-transport-classes-kqueue-4.1.110.Final.jar;%APP_HOME%\lib\netty-transport-native-unix-common-4.1.110.Final.jar;%APP_HOME%\lib\netty-codec-socks-4.1.110.Final.jar;%APP_HOME%\lib\netty-codec-dns-4.1.110.Final.jar;%APP_HOME%\lib\netty-codec-4.1.110.Final.jar;%APP_HOME%\lib\netty-transport-4.1.110.Final.jar;%APP_HOME%\lib\netty-resolver-4.1.110.Final.jar;%APP_HOME%\lib\netty-buffer-4.1.110.Final.jar;%APP_HOME%\lib\netty-common-4.1.110.Final.jar;%APP_HOME%\lib\reactor-core-3.6.4.jar;%APP_HOME%\lib\hbase-protocol-shaded-2.5.9.jar;%APP_HOME%\lib\hbase-shaded-protobuf-4.1.5.jar;%APP_HOME%\lib\httpclient-4.5.13.jar;%APP_HOME%\lib\commons-codec-1.15.jar;%APP_HOME%\lib\commons-io-2.14.0.jar;%APP_HOME%\lib\commons-configuration2-2.8.0.jar;%APP_HOME%\lib\commons-text-1.10.0.jar;%APP_HOME%\lib\commons-lang3-3.12.0.jar;%APP_HOME%\lib\hbase-shaded-miscellaneous-4.1.5.jar;%APP_HOME%\lib\hbase-shaded-netty-4.1.5.jar;%APP_HOME%\lib\opentelemetry-semconv-1.15.0-alpha.jar;%APP_HOME%\lib\opentelemetry-api-1.15.0.jar;%APP_HOME%\lib\joni-2.1.31.jar;%APP_HOME%\lib\jcodings-1.0.55.jar;%APP_HOME%\lib\commons-crypto-1.1.0.jar;%APP_HOME%\lib\zookeeper-jute-3.8.4.jar;%APP_HOME%\lib\audience-annotations-0.13.0.jar;%APP_HOME%\lib\error_prone_annotations-2.26.1.jar;%APP_HOME%\lib\perfmark-api-0.26.0.jar;%APP_HOME%\lib\jsr305-3.0.2.jar;%APP_HOME%\lib\simpleclient_tracer_otel-0.16.0.jar;%APP_HOME%\lib\simpleclient_tracer_otel_agent-0.16.0.jar;%APP_HOME%\lib\failureaccess-1.0.2.jar;%APP_HOME%\lib\listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar;%APP_HOME%\lib\checker-qual-3.42.0.jar;%APP_HOME%\lib\logback-core-1.5.6.jar;%APP_HOME%\lib\hadoop-shaded-protobuf_3_21-1.2.0.jar;%APP_HOME%\lib\hadoop-shaded-guava-1.2.0.jar;%APP_HOME%\lib\commons-cli-1.5.0.jar;%APP_HOME%\lib\commons-math3-3.6.1.jar;%APP_HOME%\lib\commons-net-3.9.0.jar;%APP_HOME%\lib\commons-beanutils-1.9.4.jar;%APP_HOME%\lib\commons-collections-3.2.2.jar;%APP_HOME%\lib\jetty-webapp-9.4.53.v20231009.jar;%APP_HOME%\lib\jetty-servlet-9.4.53.v20231009.jar;%APP_HOME%\lib\jetty-security-9.4.53.v20231009.jar;%APP_HOME%\lib\jetty-server-9.4.53.v20231009.jar;%APP_HOME%\lib\javax.servlet-api-3.1.0.jar;%APP_HOME%\lib\jackson-jaxrs-json-provider-2.15.3.jar;%APP_HOME%\lib\jackson-jaxrs-base-2.15.3.jar;%APP_HOME%\lib\jackson-databind-2.15.3.jar;%APP_HOME%\lib\jackson-core-2.15.3.jar;%APP_HOME%\lib\jackson-annotations-2.15.3.jar;%APP_HOME%\lib\jackson-module-jaxb-annotations-2.15.3.jar;%APP_HOME%\lib\jakarta.xml.bind-api-2.3.3.jar;%APP_HOME%\lib\jakarta.activation-api-1.2.2.jar;%APP_HOME%\lib\websocket-client-9.4.53.v20231009.jar;%APP_HOME%\lib\jetty-client-9.4.53.v20231009.jar;%APP_HOME%\lib\jetty-http-9.4.53.v20231009.jar;%APP_HOME%\lib\websocket-common-9.4.53.v20231009.jar;%APP_HOME%\lib\jetty-io-9.4.53.v20231009.jar;%APP_HOME%\lib\jetty-util-ajax-9.4.53.v20231009.jar;%APP_HOME%\lib\jetty-xml-9.4.53.v20231009.jar;%APP_HOME%\lib\jetty-util-9.4.53.v20231009.jar;%APP_HOME%\lib\jsp-api-2.1.jar;%APP_HOME%\lib\jersey-guice-1.19.4.jar;%APP_HOME%\lib\jersey-servlet-1.19.4.jar;%APP_HOME%\lib\jersey-json-1.20.jar;%APP_HOME%\lib\jettison-1.5.4.jar;%APP_HOME%\lib\reload4j-1.2.22.jar;%APP_HOME%\lib\re2j-1.1.jar;%APP_HOME%\lib\gson-2.10.1.jar;%APP_HOME%\lib\jsch-0.1.55.jar;%APP_HOME%\lib\curator-recipes-5.2.0.jar;%APP_HOME%\lib\curator-framework-5.2.0.jar;%APP_HOME%\lib\curator-client-5.2.0.jar;%APP_HOME%\lib\commons-compress-1.24.0.jar;%APP_HOME%\lib\bcprov-jdk15on-1.70.jar;%APP_HOME%\lib\stax2-api-4.2.1.jar;%APP_HOME%\lib\woodstox-core-5.4.0.jar;%APP_HOME%\lib\jaxb-impl-2.2.3-1.jar;%APP_HOME%\lib\jaxb-api-2.2.11.jar;%APP_HOME%\lib\jline-3.22.0.jar;%APP_HOME%\lib\reactive-streams-1.0.4.jar;%APP_HOME%\lib\hbase-shaded-gson-4.1.5.jar;%APP_HOME%\lib\javax.activation-api-1.2.0.jar;%APP_HOME%\lib\opentelemetry-context-1.15.0.jar;%APP_HOME%\lib\animal-sniffer-annotations-1.23.jar;%APP_HOME%\lib\annotations-4.1.1.4.jar;%APP_HOME%\lib\simpleclient_tracer_common-0.16.0.jar;%APP_HOME%\lib\nimbus-jose-jwt-9.31.jar;%APP_HOME%\lib\httpcore-4.4.13.jar;%APP_HOME%\lib\commons-logging-1.2.jar;%APP_HOME%\lib\jersey-client-1.19.4.jar;%APP_HOME%\lib\jersey-server-1.19.4.jar;%APP_HOME%\lib\jersey-core-1.19.4.jar;%APP_HOME%\lib\netty-codec-haproxy-4.1.110.Final.jar;%APP_HOME%\lib\netty-codec-memcache-4.1.110.Final.jar;%APP_HOME%\lib\netty-codec-mqtt-4.1.110.Final.jar;%APP_HOME%\lib\netty-codec-redis-4.1.110.Final.jar;%APP_HOME%\lib\netty-codec-smtp-4.1.110.Final.jar;%APP_HOME%\lib\netty-codec-stomp-4.1.110.Final.jar;%APP_HOME%\lib\netty-codec-xml-4.1.110.Final.jar;%APP_HOME%\lib\netty-handler-ssl-ocsp-4.1.110.Final.jar;%APP_HOME%\lib\netty-transport-rxtx-4.1.110.Final.jar;%APP_HOME%\lib\netty-transport-sctp-4.1.110.Final.jar;%APP_HOME%\lib\netty-transport-udt-4.1.110.Final.jar;%APP_HOME%\lib\jcip-annotations-1.0-1.jar;%APP_HOME%\lib\kerby-asn1-2.0.3.jar;%APP_HOME%\lib\kerby-util-2.0.3.jar;%APP_HOME%\lib\websocket-api-9.4.53.v20231009.jar;%APP_HOME%\lib\jsr311-api-1.1.1.jar;%APP_HOME%\lib\javax.inject-1.jar;%APP_HOME%\lib\aopalliance-1.0.jar;%APP_HOME%\lib\kerby-xdr-2.0.3.jar


@rem Execute Initial testing
"%JAVA_EXE%" %DEFAULT_JVM_OPTS% %JAVA_OPTS% %INITIAL_TESTING_OPTS%  -classpath "%CLASSPATH%" com.photon.Main %*

:end
@rem End local scope for the variables with windows NT shell
if %ERRORLEVEL% equ 0 goto mainEnd

:fail
rem Set variable INITIAL_TESTING_EXIT_CONSOLE if you need the _script_ return code instead of
rem the _cmd.exe /c_ return code!
set EXIT_CODE=%ERRORLEVEL%
if %EXIT_CODE% equ 0 set EXIT_CODE=1
if not ""=="%INITIAL_TESTING_EXIT_CONSOLE%" exit %EXIT_CODE%
exit /b %EXIT_CODE%

:mainEnd
if "%OS%"=="Windows_NT" endlocal

:omega
