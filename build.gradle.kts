plugins {
    id("java")
    id("application")
    id("io.freefair.lombok") version "9.5.0"
}

group = "dev.vuis"
version = "1.0-SNAPSHOT"

val blockfrontModVersion = project.property("blockfrontModVersion")
val blockfrontLibVersion = project.property("blockfrontLibVersion")
val extractedDir = layout.buildDirectory.dir("extracted")

val outerJar = configurations.create("outerJar")

repositories {
    maven("https://api.modrinth.com/maven")
    mavenCentral()
}

dependencies {
    outerJar("maven.modrinth:blockfront:${blockfrontModVersion}")

    compileOnly("org.jetbrains:annotations:26.0.2-1")

    implementation("org.slf4j:slf4j-api:2.0.17")
    implementation("org.apache.logging.log4j:log4j-api:2.25.2")
    runtimeOnly("org.apache.logging.log4j:log4j-slf4j2-impl:2.25.2")
    runtimeOnly("org.apache.logging.log4j:log4j-core:2.25.3")

    implementation("io.netty:netty-all:4.2.9.Final")
    implementation("com.google.guava:guava:33.5.0-jre")
    implementation("com.google.code.gson:gson:2.13.2")
    implementation("it.unimi.dsi:fastutil:8.5.18")
    implementation("net.raphimc:MinecraftAuth:5.0.0")

//    runtimeOnly("org.xerial:sqlite-jdbc:3.53.1.0")

    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
}

application {
    mainClass = "dev.vuis.bfapi.main.ApiMain"
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

val extractBlockfrontLibraryTask = tasks.register<Copy>("extractBlockfrontLibrary") {
    dependsOn(outerJar)

    from(zipTree(outerJar.resolve().first()))
    include("META-INF/jarjar/com.boehmod.blockfront.BlockFrontLibrary-${blockfrontLibVersion}.jar")
    into(extractedDir)
    eachFile {
        path = name
    }
    rename(".*", "blockfront-library.jar")
}

val blockfrontLibrary = files(
    extractBlockfrontLibraryTask.map {
        fileTree(it.destinationDir) {
            include("*.jar")
        }
    }
)

dependencies {
    implementation(blockfrontLibrary)
}

tasks.clean {
    delete(extractedDir)
}
