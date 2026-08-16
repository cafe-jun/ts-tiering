rootProject.name = "ts-tiering"

include("core", "storage-parquet", "storage-s3", "archiver", "bench")

project(":core").projectDir = file("modules/core")
project(":storage-parquet").projectDir = file("modules/storage-parquet")
project(":storage-s3").projectDir = file("modules/storage-s3")
project(":archiver").projectDir = file("modules/archiver")
project(":bench").projectDir = file("bench")
