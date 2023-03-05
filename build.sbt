import Dependencies._

ThisBuild / scalaVersion := "2.13.10"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "global.vigil"
ThisBuild / organizationName := "vigil"

lazy val root = (project in file("."))
  .settings(
    name := "vigil-test",
    libraryDependencies ++= Seq(
      sparkCore,
      sparkSql,
      awsSdk,
      scalaTest % Test))

Compile / mainClass := Some("vigil.Main")
assembly / mainClass := Some("vigil.Main")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}