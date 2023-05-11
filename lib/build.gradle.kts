@file:Suppress("PropertyName", "LocalVariableName")

import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import java.time.LocalDateTime

plugins {
    kotlin("jvm") version "1.8.20"
    kotlin("kapt") version "1.8.20"
    `java-library`
    id("org.javamodularity.moduleplugin") version "1.8.12"
}

group = "org.rationalityfrontline.ktrader"
version = "1.3.4"
val NAME = "ktrader-broker-ctp"
val DESC = "KTrader-API 中 Broker 接口的 CTP 实现"
val GITHUB_REPO = "ktrader-tech/ktrader-broker-ctp"
val asTest = false  // 是否编译为仿真评测版本

val pluginClass = "org.rationalityfrontline.ktrader.broker.ctp.CtpBrokerPlugin"
val pluginId = if (asTest) "KTB-CTP-CP" else "KTB-CTP"
val pluginVersion = version as String
val pluginRequires = "0.4.2"
val pluginDescription = DESC
val pluginProvider = "RationalityFrontline"
val pluginLicense = "Apache License 2.0"

repositories {
    mavenCentral()
    mavenLocal()
}

dependencies {
    kapt("org.pf4j:pf4j:3.9.0")
    compileOnly(kotlin("stdlib"))
    compileOnly("org.rationalityfrontline.ktrader:ktrader-api:$pluginRequires")
    compileOnly("org.rationalityfrontline.ktrader:ktrader-utils:0.1.4")
    val jctp = if (asTest) "org.rationalityfrontline:jctp:6.6.9_CP-1.0.5" else "org.rationalityfrontline:jctp:6.6.9-1.0.5"
    implementation(jctp) { exclude(group = "org.slf4j", module = "slf4j-api") }
}

sourceSets.main {
    java.srcDir("src/main/kotlin")
}

tasks {
    withType(JavaCompile::class.java) {
        options.release.set(11)
    }
    withType(KotlinCompile::class.java) {
        kotlinOptions.jvmTarget = "11"
    }
    compileKotlin {
        doFirst {  //写入 Build 信息
            File("lib/src/main/kotlin/org/rationalityfrontline/ktrader/broker/ctp/BuildInfo.kt").writeText(
                """ 
                |package org.rationalityfrontline.ktrader.broker.ctp
                |
                |object BuildInfo {
                |    const val NAME = "$NAME"
                |    const val VERSION = "$version"
                |    const val BUILD_DATE = "${LocalDateTime.now()}"
                |    const val VENDOR = "RationalityFrontline"
                |}
                """.trimMargin()
            )
        }
    }
    register<Zip>("pluginZip") {
        group = "distribution"
        archiveFileName.set("$pluginId-$pluginVersion.zip")
        into("classes") {
            with(jar.get())
        }
        into("lib") {
            from(configurations.runtimeClasspath.get().filter { it.name.endsWith("jar") })
        }
    }
    jar {
        manifest.attributes(mapOf(
            "Implementation-Title" to NAME,
            "Implementation-Version" to project.version,
            "Implementation-Vendor" to "RationalityFrontline",
            "Plugin-Class" to pluginClass,
            "Plugin-Id" to pluginId,
            "Plugin-Version" to pluginVersion,
            "Plugin-Requires" to pluginRequires,
            "Plugin-Description" to pluginDescription,
            "Plugin-Provider" to pluginProvider,
            "Plugin-License" to pluginLicense
        ))
    }
}