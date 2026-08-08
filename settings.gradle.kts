rootProject.name = "ts-tiering"

include("core", "bench")

project(":core").projectDir = file("modules/core")
project(":bench").projectDir = file("bench")
