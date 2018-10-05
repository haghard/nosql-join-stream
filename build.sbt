import de.heikoseeberger.sbtheader.SbtHeader.autoImport.HeaderPattern
import bintray.Keys._
import com.scalapenos.sbt.prompt.SbtPrompt.autoImport._
import com.typesafe.sbt.SbtScalariform.ScalariformKeys

organization := "com.haghard"

name := "nosql-join-stream"

version := "0.2.7"

scalaVersion := "2.11.8"

parallelExecution := false
parallelExecution in Test := false
logBuffered in Test := false

initialCommands in console in Test := "import org.specs2._"

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
val CassandraDriverVersion = "3.6.0"
val ScalazStreamVersion = "0.8.5"
val RxScala = "0.25.0" //doesn't work with 0.26.2, need to figure out
val Logback = "1.1.2"
val Akka = "2.5.17"

val localMvnRepo = "/Volumes/Data/dev_build_tools/apache-maven-3.1.1/repository"

scalariformSettings

ScalariformKeys.preferences in Compile  := formattingPreferences
ScalariformKeys.preferences in Test     := formattingPreferences

resolvers ++= Seq(
  Resolver.typesafeRepo("releases"),
  "Scalaz" at "http://dl.bintray.com/scalaz/releases",
  "Local Maven Repository" at "file:///" + localMvnRepo
)

libraryDependencies ++= Seq(
  "org.mongodb"              %   "mongo-java-driver"        %   MongoDriverVersion     withSources(),
  "com.datastax.cassandra"   %   "cassandra-driver-core"    %   CassandraDriverVersion withSources(),
  "org.scalaz.stream"        %%  "scalaz-stream"            %   ScalazStreamVersion    withSources(),
  "io.reactivex"             %%  "rxscala"                  %   RxScala                withSources(),
  "com.typesafe.akka"        %%  "akka-stream"              %   Akka                   withSources(),
  "com.google.protobuf"      %   "protobuf-java"            %  "3.5.1"                 withSources(),
  "com.chuusai"              %%  "shapeless"                %  "2.3.3"
  //"ch.qos.logback"         %   "logback-classic"          %   Logback //should be commented for releases
)

//2.1.3.1
//JoinMongoSpec failed with specs2-core 3.6.4
libraryDependencies ++= Seq(
  "de.bwaldvogel"       %   "mongo-java-server" %   "1.7.0"   % "test" withSources(),
  "org.cassandraunit"   %   "cassandra-unit"    %   "3.0.0.1" % "test" excludeAll(ExclusionRule(organization = "ch.qos.logback")),
  "org.specs2"          %%  "specs2-core"       %   "2.4.17"  % "test" withSources(),
  "org.scalatest"       %%  "scalatest"         %   "3.0.1"   % "test",
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

repository in bintray := "releases" //"snapshot"

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


def formattingPreferences = {
  import scalariform.formatter.preferences._
  FormattingPreferences()
    .setPreference(RewriteArrowSymbols, true)
    .setPreference(AlignParameters, true)
    .setPreference(AlignSingleLineCaseStatements, true)
    .setPreference(SpacesAroundMultiImports, true)
}

cancelable in Global := true

//create/update for Compile and Test configurations, add the following settings to your build
inConfig(Compile)(compileInputs.in(compile) <<= compileInputs.in(compile).dependsOn(createHeaders.in(compile)))
inConfig(Test)(compileInputs.in(compile) <<= compileInputs.in(compile).dependsOn(createHeaders.in(compile)))

//eval System.getProperty("java.version")
//eval System.getProperty("java.home")

//bintray:: tab
//bintray::publish

//git tag -a v0.2.6 -m 'version 0.2.6'
//git push origin master --tags