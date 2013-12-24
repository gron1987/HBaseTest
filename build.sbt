import AssemblyKeys._

name := "hello"

version := "1.1"

scalaVersion := "2.9.2"

libraryDependencies ++= Seq(
    "org.slf4j" % "slf4j-log4j12" % "1.5.2",
    "org.apache.hbase" % "hbase-client" % "0.96.0-hadoop2",
    "org.apache.hbase" % "hbase-common" % "0.96.0-hadoop2",
    "org.apache.hbase" % "hbase" % "0.96.0-hadoop2",
    "org.apache.hbase" % "hbase-server" % "0.96.0-hadoop2",
    "org.apache.hbase" % "hbase-hadoop-compat" % "0.96.0-hadoop2",
    "org.apache.hadoop" % "hadoop-client" % "2.2.0",
    "org.apache.hadoop" % "hadoop-common" % "2.2.0",
    "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.2.0",
    "commons-io" % "commons-io" % "2.4"
)

resolvers += "ClouderaRepo" at "https://repository.cloudera.com/content/repositories/releases"

resolvers += "Apache HBase" at "https://repository.apache.org/content/repositories/releases"

autoCompilerPlugins := true

assemblySettings

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
 {
    case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
    case _ => MergeStrategy.first
 }
}