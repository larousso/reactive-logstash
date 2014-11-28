import scalariform.formatter.preferences._


name := """reactive-logstash"""

version := "1.0"

scalaVersion := "2.11.2"


// Uncomment to use Akka
libraryDependencies ++= Seq(
  "org.scalatest" % "scalatest_2.11" % "2.2.1",
  "commons-io" % "commons-io" % "2.4",
  "org.reactivestreams" % "reactive-streams-spi" % "0.3",
  "org.reactivestreams" % "reactive-streams-tck" % "0.4.0",
  "com.typesafe.akka" %% "akka-stream-experimental" % "0.11",
  "com.typesafe.play" %% "play-ws" % "2.3.6",
  "com.typesafe.play" %% "play-json" % "2.3.6",
  "com.typesafe.akka" %% "akka-testkit" % "2.3.6",
  "com.livestream" %% "scredis" % "2.0.5"
)

addCommandAlias("generate-project", ";update-classifiers;update-sbt-classifiers;gen-idea sbt-classifiers")

scalariformSettings

ScalariformKeys.preferences := ScalariformKeys.preferences.value
  .setPreference(AlignSingleLineCaseStatements, true)
  .setPreference(AlignSingleLineCaseStatements.MaxArrowIndent, 100)
  .setPreference(DoubleIndentClassDeclaration, true)
  .setPreference(PreserveDanglingCloseParenthesis, true)

scalacOptions in Test ++= Seq("-Yrangepos")




