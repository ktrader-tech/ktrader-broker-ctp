import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.9.22"
    application
}

repositories {
    mavenCentral()
    mavenLocal()
}

dependencies {
    implementation("org.rationalityfrontline.ktrader:ktrader-api:0.4.3")
    implementation("org.rationalityfrontline.ktrader:ktrader-utils:0.1.5")
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
