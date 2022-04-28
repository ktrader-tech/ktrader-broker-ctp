@file:Suppress("PropertyName", "LocalVariableName")

import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import java.time.LocalDateTime

plugins {
    kotlin("jvm") version "1.6.10"
    kotlin("kapt") version "1.6.10"
    `java-library`
    `maven-publish`
    signing
    id("org.jetbrains.dokka") version "1.6.0"
    id("org.javamodularity.moduleplugin") version "1.8.10"
}

group = "org.rationalityfrontline.ktrader"
version = "1.3.1"
val NAME = "ktrader-broker-ctp"
val DESC = "KTrader-API 中 Broker 接口的 CTP 实现"
val GITHUB_REPO = "ktrader-tech/ktrader-broker-ctp"
val asPlugin = true  // 是否作为插件编译

val pluginClass = "org.rationalityfrontline.ktrader.broker.ctp.CtpBrokerPlugin"
val pluginId = "KTB-CTP"
val pluginVersion = version as String
val pluginRequires = "0.2.0"
val pluginDescription = DESC
val pluginProvider = "RationalityFrontline"
val pluginLicense = "Apache License 2.0"

repositories {
    mavenCentral()
    mavenLocal()
}

dependencies {
    val pf4j = "org.rationalityfrontline.workaround:pf4j:3.7.1"
    val ktrader_api = "org.rationalityfrontline.ktrader:ktrader-api:$pluginRequires"
    val jctp = "org.rationalityfrontline:jctp:6.6.1_P1-1.0.4"
    if (asPlugin) {  // 发布为 ZIP 插件
        compileOnly(kotlin("stdlib"))
        compileOnly(ktrader_api)
        compileOnly(pf4j)
        kapt(pf4j)
        implementation(jctp) { exclude(group = "org.slf4j", module = "slf4j-api") }
    } else {  // 发布到 Maven 仓库
        api(ktrader_api)
        compileOnly(pf4j)
        implementation(jctp)
    }
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
                |    const val IS_PLUGIN = $asPlugin
                |}
                """.trimMargin()
            )
        }
    }
    dokkaHtml {
        outputDirectory.set(buildDir.resolve("javadoc"))
        moduleName.set("KTrader-Broker-CTP")
        dokkaSourceSets {
            named("main") {
                includes.from("module.md")
            }
        }
    }
    register<Jar>("javadocJar") {
        archiveClassifier.set("javadoc")
        from(dokkaHtml)
    }
    register<Jar>("sourcesJar") {
        archiveClassifier.set("sources")
        from(sourceSets["main"].allSource)
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

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            artifact(tasks["sourcesJar"])
            artifact(tasks["javadocJar"])
            pom {
                name.set(NAME)
                description.set(DESC)
                artifactId = NAME
                packaging = "jar"
                url.set("https://github.com/$GITHUB_REPO")
                licenses {
                    license {
                        name.set("The Apache Software License, Version 2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                    }
                }
                developers {
                    developer {
                        name.set("RationalityFrontline")
                        email.set("rationalityfrontline@gmail.com")
                        organization.set("ktrader-tech")
                        organizationUrl.set("https://github.com/ktrader-tech")
                    }
                }
                scm {
                    connection.set("scm:git:git://github.com/$GITHUB_REPO.git")
                    developerConnection.set("scm:git:ssh://github.com:$GITHUB_REPO.git")
                    url.set("https://github.com/$GITHUB_REPO/tree/master")
                }
            }
        }
    }
    repositories {
        fun env(propertyName: String): String {
            return if (project.hasProperty(propertyName)) {
                project.property(propertyName) as String
            } else "Unknown"
        }
        maven {
            val releasesRepoUrl = uri("https://oss.sonatype.org/service/local/staging/deploy/maven2/")
            val snapshotsRepoUrl = uri("https://oss.sonatype.org/content/repositories/snapshots/")
            url = if (version.toString().endsWith("SNAPSHOT")) snapshotsRepoUrl else releasesRepoUrl
            credentials {
                username = env("ossrhUsername")
                password = env("ossrhPassword")
            }
        }
    }
}

signing {
    sign(publishing.publications["maven"])
}
