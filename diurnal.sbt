name := "DTR-Spark"

organization := "se.kth.climate.fast"

version := "1.1"

scalaVersion := "2.10.6"

//resolvers += Resolver.mavenLocal
resolvers := Resolver.mavenLocal +: resolvers.value

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.1" % "provided"
libraryDependencies += "com.databricks" %% "spark-csv" % "1.3.0"
libraryDependencies += "com.twitter" %% "bijection-core" % "0.9.2"
libraryDependencies += "se.kth.climate.fast" % "common" % "1.2"

mainClass in assembly := Some("se.kth.climate.fast.dtr.DTR")

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)