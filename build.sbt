import com.typesafe.sbt.SbtScalariform.ScalariformKeys
import scalariform.formatter.preferences._
import bintray.Keys._

organization := "com.haghard"

name := "nosql-join-stream"

version := "0.0.1-snapshot"

scalaVersion := "2.11.7"

parallelExecution := false
parallelExecution in Test := false
logBuffered in Test := false

initialCommands in console in Test := "import org.specs2._"

shellPrompt := { state => System.getProperty("user.name") + "> " }

scalacOptions ++= Seq(
  "-feature",
  "-deprecation",
  "-language:implicitConversions",
  "-language:higherKinds",
  "-language:existentials",
  "-language:postfixOps",
  "-language:reflectiveCalls",
  "-Yno-adapted-args",
  "-target:jvm-1.8"
)

useJGit
enablePlugins(GitVersioning)

val MongoDriverVersion = "3.0.2"
val CassandraDriverVersion = "2.1.7"
val ScalazStreamVersion = "0.8"
val RxScala = "0.25.0"
val AkkaStreams = "1.0"
val Akka = "2.4.0"
val Log4J2 = "2.4"

val localMvnRepo = "/Volumes/Data/dev_build_tools/apache-maven-3.1.1/repository"

scalariformSettings

ScalariformKeys.preferences := ScalariformKeys.preferences.value
  .setPreference(RewriteArrowSymbols, true)
  .setPreference(AlignParameters, true)
  .setPreference(AlignSingleLineCaseStatements, true)

net.virtualvoid.sbt.graph.Plugin.graphSettings

resolvers ++= Seq(
  "Local Maven Repository" at "file:///" + localMvnRepo,
  "Scalaz" at "http://dl.bintray.com/scalaz/releases"
)

libraryDependencies ++= Seq(
    "org.mongodb"            %   "mongo-java-driver"        %   MongoDriverVersion     withSources(),
    "com.datastax.cassandra" %   "cassandra-driver-core"    %   CassandraDriverVersion withSources(),
    "org.scalaz.stream"      %%  "scalaz-stream"            %   ScalazStreamVersion    withSources(),
    "io.reactivex"           %%  "rxscala"                  %   RxScala                withSources(),
    "com.typesafe.akka"      %%  "akka-stream-experimental" %   AkkaStreams            withSources(),
    "org.apache.logging.log4j"%   "log4j-api"               %   Log4J2,
    "org.apache.logging.log4j"%   "log4j-core"              %   Log4J2
)

libraryDependencies ++= Seq(
  "de.bwaldvogel"     %   "mongo-java-server" %   "1.4.4"   % "test" withSources(),
  "org.cassandraunit" %   "cassandra-unit"    %   "2.0.2.2" % "test",
  "org.specs2"        %%  "specs2-core"       %   "3.6.4"   % "test" withSources(),
  "org.scalatest"     %%  "scalatest"         %   "2.2.5"   % "test",
  "com.typesafe.akka" %%  "akka-testkit" % Akka % "test"
)

scalacOptions ++= Seq(
  "-encoding", "UTF-8",
  "-target:jvm-1.8",
  "-deprecation",
  "-unchecked",
  "-Ywarn-dead-code",
  "-feature",
  "-language:implicitConversions",
  "-language:postfixOps",
  "-language:existentials")

javacOptions ++= Seq(
  "-source", "1.8",
  "-target", "1.8",
  "-Xlint:unchecked",
  "-Xlint:deprecation")

seq(bintraySettings:_*)

licenses += ("Apache-2.0", url("http://www.apache.org/licenses/"))

bintrayOrganization in bintray := Some("haghard")

repository in bintray := "snapshots" //"releases"

publishMavenStyle := true
//publishTo := Some(Resolver.file("file",  new File(localMvnRepo)))

//sbt createHeaders
headers := Map(
  "scala" -> (
    HeaderPattern.cStyleBlockComment,
   """|/*
      | * Licensed under the Apache License, Version 2.0 (the "License");
      | * you may not use this file except in compliance with the License.
      | * You may obtain a copy of the License at
      | *
      | *    http://www.apache.org/licenses/LICENSE-2.0
      | *
      | * Unless required by applicable law or agreed to in writing, software
      | * distributed under the License is distributed on an "AS IS" BASIS,
      | * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
      | * See the License for the specific language governing permissions and
      | * limitations under the License.
      | */
      |
      |""".stripMargin
    )
)

//create/update for Compile and Test configurations, add the following settings to your build
inConfig(Compile)(compileInputs.in(compile) <<= compileInputs.in(compile).dependsOn(createHeaders.in(compile)))
inConfig(Test)(compileInputs.in(compile) <<= compileInputs.in(compile).dependsOn(createHeaders.in(compile)))

//eval System.getProperty("java.version")
//eval System.getProperty("java.home")

//bintray:: tab
//bintray::publish