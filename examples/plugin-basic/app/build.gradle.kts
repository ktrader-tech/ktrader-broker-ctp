plugins {
    kotlin("jvm") version "1.5.30"
    application
}

repositories {
    mavenCentral()
    mavenLocal()
}

dependencies {
    implementation("org.rationalityfrontline.workaround:pf4j:3.7.0")
    implementation("org.rationalityfrontline.ktrader:ktrader-api:0.1.0")
}

application {
    mainClass.set("com.example.basic.AppKt")
}
