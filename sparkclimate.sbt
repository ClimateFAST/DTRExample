name := "Spark Climate"

organization := "se.kth.climate.fast"

version := "1.2-SNAPSHOT"

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
libraryDependencies += ("io.hops" % "hadoop-client" % "2.7.3").
                          exclude("commons-beanutils", "commons-beanutils").
                          exclude("commons-beanutils", "commons-beanutils-core").
                          exclude("commons-collections", "commons-collections")
libraryDependencies += "commons-beanutils" % "commons-beanutils" % "1.9.3"
libraryDependencies += "commons-collections" % "commons-collections" % "3.2.2"
libraryDependencies += "org.scalactic" %% "scalactic" % "2.2.6"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"
libraryDependencies += "se.kth.climate.fast" % "netcdf-hdfs-hadoop" % "0.6-SNAPSHOT"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.7" % "test"
libraryDependencies += "org.scalanlp" %% "breeze" % "0.12"
libraryDependencies += "edu.ucar" % "httpservices" % "4.6.4"
libraryDependencies += "com.google.code.findbugs" % "jsr305" % "2.0.3"
libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "2.0.1_0.6.0" % "test"
libraryDependencies += "se.kth.climate.fast" % "netcdf-hdfs-aligner" % "0.6-SNAPSHOT" % "test"


parallelExecution in Test := false

/*
mainClass in assembly := Some("se.kth.climate.fast.netcdf.Debug")
test in assembly := {}
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
assemblyMergeStrategy in assembly := {
  case "application.conf"                            => MergeStrategy.concat
  case "log4j.properties"                            => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
*/


publishMavenStyle := true
publishTo := {
  val name = "Kompics-Repository"
  val url = "kompics.i.sics.se"
  val prefix = "/home/maven/"
  import java.io.File
  val privateKeyFile: File = new File(sys.env("HOME") + "/.ssh/id_rsa")
  if (isSnapshot.value)
    Some(Resolver.ssh(name, url, prefix + "snapshotrepository") as("root", privateKeyFile) withPermissions("0644")) 
  else
    Some(Resolver.ssh(name, url, prefix + "repository") as("root", privateKeyFile) withPermissions("0644")) 
}
resolvers ++= Seq(
    {
        import java.io.File
        val privateKeyFile: File = new File(sys.env("HOME") + "/.ssh/id_rsa")
        Resolver.ssh("Kompics Repository", "kompics.i.sics.se") as("root", privateKeyFile) withPermissions("0644")
    }
)