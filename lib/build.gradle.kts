plugins {
    kotlin("jvm") version "1.4.20"
    kotlin("kapt") version "1.4.20"
    `java-library`
    `maven-publish`
    signing
    id("org.jetbrains.dokka") version "1.4.10"
    id("org.javamodularity.moduleplugin") version "1.7.0"
}

group = "org.rationalityfrontline.ktrader"
version = "0.1.0-SNAPSHOT"
val NAME = "ktrader-broker-ctp"
val DESC = "CTP implementation of KTrader-Broker-API"
val GITHUB_REPO = "RationalityFrontline/ktrader-broker-ctp"

val pluginClass = "org.rationalityfrontline.ktrader.broker.ctp.CtpBrokerPlugin"
val pluginId = "broker-ctp-rf"
val pluginVersion = "0.1.0-SNAPSHOT"
val pluginRequires = "0.1.1-SNAPSHOT"
val pluginDescription = DESC
val pluginProvider = "RationalityFrontline"
val pluginLicense = "Apache License 2.0"

repositories {
    mavenLocal()
    mavenCentral()
    jcenter()
}

dependencies {
    compileOnly(kotlin("stdlib"))
    compileOnly("org.rationalityfrontline.ktrader:ktrader-broker-api:0.1.1-SNAPSHOT")
    kapt("org.pf4j:pf4j:3.4.1")
    implementation("org.rationalityfrontline:jctp:6.3.19-1.0.0")
    testImplementation(platform("org.junit:junit-bom:5.7.0"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.7.0")
}

configurations.runtime {
    exclude(group = "org.slf4j", module = "slf4j-api")
}

sourceSets.main {
    java.srcDir("src/main/kotlin")
}

tasks {
    kapt{
        javacOptions {
            option("--module-path", compileJava.get().classpath.asPath)
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
    register<Jar>("pluginZip") {
        archiveFileName.set("$pluginId-$pluginVersion.zip")
        into("classes") {
            with(jar.get())
        }
        into("lib") {
            from({
                configurations.runtimeClasspath.get().filter { it.name.endsWith("jar") }
            })
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
                        organization.set("RationalityFrontline")
                        organizationUrl.set("https://github.com/RationalityFrontline")
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
