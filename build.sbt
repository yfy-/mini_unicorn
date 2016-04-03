lazy val root = (project in file(".")).
  settings(
    organization := "com.yfy", 
    name := "mini-unicorn",
    version := "0.0.1",
    scalaVersion := "2.11.8"
  )

//unmanagedResourceDirectories in Compile <++= baseDirectory { base =>
//    Seq( base / "src/main/resources/small-fake", base /  "src/main/resources/soc-pokec")
//}

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.1"
)

