plugins {
    kotlin("jvm") version "1.5.21"
    kotlin("kapt") version "1.5.21"
    `java-library`
    `maven-publish`
    signing
    id("org.jetbrains.dokka") version "1.5.0"
    id("org.javamodularity.moduleplugin") version "1.8.7"
}

group = "org.rationalityfrontline.ktrader"
version = "1.1.2"
val NAME = "ktrader-broker-ctp"
val DESC = "CTP implementation of KTrader-Broker-API"
val GITHUB_REPO = "ktrader-tech/ktrader-broker-ctp"

val pluginClass = "org.rationalityfrontline.ktrader.broker.ctp.CtpBrokerPlugin"
val pluginId = "broker-ctp-rf"
val pluginVersion = version as String
val pluginRequires = "1.1.2"
val pluginDescription = DESC
val pluginProvider = "RationalityFrontline"
val pluginLicense = "Apache License 2.0"

repositories {
    mavenCentral()
}

dependencies {
    val publishMaven = true  // 是否发布到 Maven 仓库
    val depCoroutines = "org.jetbrains.kotlinx:kotlinx-coroutines-core:1.5.1"
    val depPf4j = "org.rationalityfrontline.workaround:pf4j:3.7.0"
    val depKtraderBrokerApi = "org.rationalityfrontline.ktrader:ktrader-broker-api:$pluginRequires"
    val depJctp = "org.rationalityfrontline:jctp:6.6.1_P1-1.0.0"
    if (publishMaven) {  // 发布到 Maven 仓库
        implementation(kotlin("stdlib"))
        api(depCoroutines)
        api(depKtraderBrokerApi)
        compileOnly(depPf4j)
        implementation(depJctp)
    } else {  // 发布为 ZIP 插件
        compileOnly(kotlin("stdlib"))
        compileOnly(depCoroutines)
        compileOnly(depKtraderBrokerApi)
        compileOnly(depPf4j)
        kapt(depPf4j)
        implementation(depJctp) {
            exclude(group = "org.slf4j", module = "slf4j-api")
        }
    }
}

sourceSets.main {
    java.srcDir("src/main/kotlin")
}

tasks {
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
            "Plugin-Class" to pluginClass,
            "Plugin-Id" to pluginId,
            "Plugin-Version" to pluginVersion,
            "Plugin-Requires" to pluginRequires,
            "Plugin-Description" to pluginDescription,
            "Plugin-Provider" to pluginProvider,
            "Plugin-License" to pluginLicense
        ))
    }
    test {
        testLogging.showStandardStreams = true
        useJUnitPlatform {
            jvmArgs = listOf(
                "--add-exports", "org.junit.platform.commons/org.junit.platform.commons.util=ALL-UNNAMED",
                "--add-exports", "org.junit.platform.commons/org.junit.platform.commons.logging=ALL-UNNAMED"
            )
        }
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
                        url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
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
