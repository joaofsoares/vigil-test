import sbt._

object Dependencies {
  lazy val sparkCore = "org.apache.spark" %% "spark-core" % "3.3.2"
  lazy val sparkSql = "org.apache.spark" %% "spark-sql" % "3.3.2"
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.2.15"
  lazy val awsSdk = "com.amazonaws" % "aws-java-sdk" % "1.12.420"
}
