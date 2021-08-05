plugins {
    kotlin("jvm") version "1.5.21"
    application
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.rationalityfrontline.workaround:pf4j:3.7.0")
    implementation("org.rationalityfrontline.ktrader:ktrader-broker-api:1.1.3")
}

application {
    mainClass.set("com.example.basic.AppKt")
}
