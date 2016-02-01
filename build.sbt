import com.typesafe.sbt.SbtGit.git
import com.typesafe.sbt.SbtScalariform.ScalariformKeys
import scalariform.formatter.preferences._
import bintray.Keys._
import com.scalapenos.sbt.prompt.SbtPrompt.autoImport._

organization := "com.haghard"

name := "nosql-join-stream"

version := "0.1.14-SNAPSHOT"

scalaVersion := "2.11.7"

parallelExecution := false
parallelExecution in Test := false
logBuffered in Test := false

initialCommands in console in Test := "import org.specs2._"

//useJGit
//enablePlugins(GitVersioning)
//shellPrompt := { state => System.getProperty("user.name") + "> " }
//showCurrentGitBranch
//versionWithGit
//git.baseVersion := "master"

promptTheme := ScalapenosTheme

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

val MongoDriverVersion = "3.0.2"
val CassandraDriverVersion = "3.0.0"
val ScalazStreamVersion = "0.8"
val RxScala = "0.25.0"
val Logback = "1.1.2"
val Akka="2.4.2-RC1"

val localMvnRepo = "/Volumes/Data/dev_build_tools/apache-maven-3.1.1/repository"

scalariformSettings

ScalariformKeys.preferences := ScalariformKeys.preferences.value
  .setPreference(RewriteArrowSymbols, true)
  .setPreference(AlignParameters, true)
  .setPreference(AlignSingleLineCaseStatements, true)

resolvers ++= Seq(
  "Local Maven Repository" at "file:///" + localMvnRepo,
  "Scalaz" at "http://dl.bintray.com/scalaz/releases"
)

libraryDependencies ++= Seq(
  "org.mongodb"            %   "mongo-java-driver"        %   MongoDriverVersion     withSources(),
  "com.datastax.cassandra" %   "cassandra-driver-core"    %   CassandraDriverVersion withSources(),
  "org.scalaz.stream"      %%  "scalaz-stream"            %   ScalazStreamVersion    withSources(),
  "io.reactivex"           %%  "rxscala"                  %   RxScala                withSources(),
  "com.typesafe.akka"      %%  "akka-stream"              %   Akka                   withSources(),
  "com.google.protobuf"    %   "protobuf-java"            %  "2.5.0"                 withSources()
  //"ch.qos.logback"         %   "logback-classic"          %   Logback //should be commented for releases
)


//JoinMongoSpec failed with specs2-core 3.6.4
libraryDependencies ++= Seq(
  "de.bwaldvogel"       %   "mongo-java-server" %   "1.4.4"   % "test" withSources(),
  "org.cassandraunit"   %   "cassandra-unit"      %   "2.0.2.2" % "test" excludeAll(ExclusionRule(organization = "ch.qos.logback")),
  "org.specs2"          %%  "specs2-core"       %   "2.4.17"  % "test" withSources(),
  "org.scalatest"       %%  "scalatest"         %   "2.2.5"   % "test",
  "com.typesafe.akka"   %%  "akka-testkit"      %   Akka      % "test",
  "com.typesafe.akka"   %%  "akka-slf4j"        %   Akka      % "test"
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

repository in bintray := "snapshot" //"releases"

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

cancelable in Global := true

//create/update for Compile and Test configurations, add the following settings to your build
inConfig(Compile)(compileInputs.in(compile) <<= compileInputs.in(compile).dependsOn(createHeaders.in(compile)))
inConfig(Test)(compileInputs.in(compile) <<= compileInputs.in(compile).dependsOn(createHeaders.in(compile)))

//eval System.getProperty("java.version")
//eval System.getProperty("java.home")

//bintray:: tab
//bintray::publish