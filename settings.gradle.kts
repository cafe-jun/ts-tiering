rootProject.name = "ts-tiering"

include("core", "storage-parquet", "bench")

project(":core").projectDir = file("modules/core")
project(":storage-parquet").projectDir = file("modules/storage-parquet")
project(":bench").projectDir = file("bench")
