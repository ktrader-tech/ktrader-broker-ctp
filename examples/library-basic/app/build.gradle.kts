plugins {
    kotlin("jvm") version "1.5.30"
    application
}

repositories {
    mavenCentral()
    mavenLocal()
}

dependencies {
    implementation("org.rationalityfrontline.ktrader:ktrader-broker-ctp:1.2.0")
    // 如果需要使用其它版本的 JCTP，取消注释下面一行，并填入自己需要的版本号
//    implementation("org.rationalityfrontline:jctp") { version { strictly("6.6.1_P1_CP-1.0.1") } }
}

application {
    mainClass.set("com.example.basic.AppKt")
}
