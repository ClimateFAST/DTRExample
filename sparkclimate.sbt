name := "Spark Climate"

organization := "se.kth.climate.fast"

version := "1.0"

scalaVersion := "2.11.8"

resolvers += Resolver.mavenLocal
resolvers += "Kompics Releases" at "http://kompics.sics.se/maven/repository/"
resolvers += "Kompics Snapshots" at "http://kompics.sics.se/maven/snapshotrepository/"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.1" % "provided"
libraryDependencies += "com.google.code.gson" % "gson" % "2.7"
//libraryDependencies += "com.databricks" %% "spark-csv" % "1.3.0"
libraryDependencies += "com.twitter" %% "bijection-core" % "0.9.2"
libraryDependencies += "se.kth.climate.fast" % "common" % "1.4" exclude("com.fasterxml.jackson.core", "jackson-core") exclude("com.fasterxml.jackson.core", "jackson-annotations") exclude("com.fasterxml.jackson.core", "jackson-databind") exclude("org.codehaus.jackson", "jackson-core-asl") exclude("org.codehaus.jackson", "jackson-mapper-asl")
libraryDependencies += "io.hops" % "hadoop-client" % "2.7.3"
libraryDependencies += "org.scalactic" %% "scalactic" % "2.2.6"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"
libraryDependencies += "se.kth.climate.fast" % "netcdf-hdfs-hadoop" % "0.5-SNAPSHOT"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.7" % "test"
libraryDependencies += "org.scalanlp" %% "breeze" % "0.12"
libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "2.0.1_0.6.0" % "test"
libraryDependencies += "se.kth.climate.fast" % "netcdf-hdfs-aligner" % "0.5-SNAPSHOT" % "test"

parallelExecution in Test := false

//mainClass in assembly := Some("se.kth.climate.fast.dtr.DTR")

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)