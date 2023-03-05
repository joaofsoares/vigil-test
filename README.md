# Vigil Test

## Introduction

Vigil Test using Spark

## Environment

Spark = 3.3.2

## Build

`sbt compile`

## Test

`sbt test`

## Package

`sbt assembly`

## Execution

### Cluster

Start Master = `sbin/start-master.sh`

To access UI page = `http://localhost:8080`

Start Worker = `sbin/start-worker spark://localhost:7077`

#### Run

`./spark-submit --class vigil.Main --master spark://localhost:7077 vigil-test-assembly-0.1.0-SNAPSHOT.jar inputDir outputDir profile`
