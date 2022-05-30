import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.6.10"
    application
}

repositories {
    mavenCentral()
    mavenLocal()
}

dependencies {
    implementation("org.rationalityfrontline.workaround:pf4j:3.7.1")
    implementation("org.rationalityfrontline.ktrader:ktrader-api:0.2.0")
}

application {
    mainClass.set("com.example.basic.AppKt")
}

tasks {
    withType(JavaCompile::class.java) {
        options.release.set(11)
    }
    withType(KotlinCompile::class.java) {
        kotlinOptions.jvmTarget = "11"
    }
}
