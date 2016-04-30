lazy val root = (project in file(".")).
  settings(
    organization := "com.yfy", 
    name := "mini_unicorn",
    version := "0.0.1",
    scalaVersion := "2.11.8"
  )

mappings in (Compile, packageBin) ~= { _.filter(!_._1.getName.endsWith(".csv")) }

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.1" % "provided",
  "org.apache.spark" %% "spark-graphx" % "1.6.1" % "provided"
)

