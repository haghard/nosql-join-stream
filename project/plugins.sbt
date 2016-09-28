resolvers += Resolver.url(
  "bintray-sbt-plugin-releases",
  url("http://dl.bintray.com/content/sbt/sbt-plugin-releases"))(
    Resolver.ivyStylePatterns)

addSbtPlugin("me.lessis" % "bintray-sbt" % "0.2.1")
addSbtPlugin("org.scalariform"   % "sbt-scalariform" % "1.6.0")
addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "1.6.0")
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.8.1")
addSbtPlugin("de.heikoseeberger" % "sbt-header" % "1.1.1")
addSbtPlugin("com.scalapenos" % "sbt-prompt" % "0.2.1")
addSbtPlugin("com.typesafe.sbt"  % "sbt-git" % "0.8.5")