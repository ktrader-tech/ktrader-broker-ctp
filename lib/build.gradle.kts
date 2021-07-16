import org.gradle.kotlin.dsl.support.zipTo

plugins {
    kotlin("jvm") version "1.5.21"
    kotlin("kapt") version "1.5.21"
    `java-library`
    `maven-publish`
    signing
    id("org.jetbrains.dokka") version "1.4.32"
    id("org.javamodularity.moduleplugin") version "1.8.7"
}

group = "org.rationalityfrontline.ktrader"
version = "0.1.3-SNAPSHOT"
val NAME = "ktrader-broker-ctp"
val DESC = "CTP implementation of KTrader-Broker-API"
val GITHUB_REPO = "RationalityFrontline/ktrader-broker-ctp"

val pluginClass = "org.rationalityfrontline.ktrader.broker.ctp.CtpBrokerPlugin"
val pluginId = "broker-ctp-rf"
val pluginVersion = version as String
val pluginRequires = "0.1.0"
val pluginDescription = DESC
val pluginProvider = "RationalityFrontline"
val pluginLicense = "Apache License 2.0"

repositories {
    mavenLocal()
    mavenCentral()
    jcenter()
}

configurations.create("mrjar9")

dependencies {
    val pf4j = "org.pf4j:pf4j:3.7.0-SNAPSHOT"
    val ktraderBrokerApi = "org.rationalityfrontline.ktrader:ktrader-broker-api:0.1.6-SNAPSHOT"
    compileOnly(kotlin("stdlib"))
    compileOnly(ktraderBrokerApi)
    compileOnly(pf4j)
    kapt(pf4j)
    add("mrjar9", pf4j)
    implementation("org.rationalityfrontline:jctp:6.6.1_P1-1.0.0") {
        exclude(group = "org.slf4j", module = "slf4j-api")
    }
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.5.1")
    testImplementation(pf4j)
    testImplementation(ktraderBrokerApi)
    testImplementation(platform("org.junit:junit-bom:5.7.0"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.7.0")
    testImplementation("org.slf4j:slf4j-api:1.7.30")
    testImplementation("org.slf4j:slf4j-simple:1.7.30")
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
    register("moveModuleInfoInMRJARs") {
        configurations["mrjar9"].files.forEach { mrjar9Jar ->
            val tree = zipTree(mrjar9Jar)
            val moduleInfoFile = tree.matching { include("module-info.class") }
            val v9ModuleInfoFile = tree.matching { include("META-INF/versions/9/module-info.class") }
            if (moduleInfoFile.isEmpty && !v9ModuleInfoFile.isEmpty) {
                val unzipDir = file("$buildDir/tmp/mrjar9")
                delete(unzipDir)
                mkdir(unzipDir)
                copy {
                    from(tree, v9ModuleInfoFile.singleFile)
                    into(unzipDir)
                }
                zipTo(mrjar9Jar, unzipDir)
                println("Fixed $mrjar9Jar")
            }
        }
    }
    // See https://github.com/java9-modularity/gradle-modules-plugin/issues/162 and https://youtrack.jetbrains.com/issue/KT-32202
    afterEvaluate {
        tasks["kaptKotlin"].dependsOn("moveModuleInfoInMRJARs")
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
