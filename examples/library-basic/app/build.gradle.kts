plugins {
    kotlin("jvm") version "1.5.21"
    application
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.rationalityfrontline.ktrader:ktrader-broker-ctp:1.1.1")
}

application {
    mainClass.set("com.example.basic.AppKt")
}
