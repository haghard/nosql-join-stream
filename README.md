Nosql-join-stream
===================

Goals:
  * To demonstrate power of design based on "Abstract algebraic data type"
  * Provide support for streaming libs:
  
    [ScalazStream](https://github.com/scalaz/scalaz-stream), 
    [AkkaStream](https://github.com/akka/akka) and 
    [RxScala](https://github.com/ReactiveX/RxScala.git)
  * Provide support for MongoDb and Cassandra
  * Resource safety

The main idea for "Abstract algebraic data type" pattern was taken from this [blog post](http://io.pellucid.com/blog/abstract-algebraic-data-type)


Where to get it
=================
```
resolvers += "haghard-bintray"  at "http://dl.bintray.com/haghard/releases"

libraryDependencies +=  "com.haghard"  %% "nosql-join-stream" % "0.2.5"

```

Log Example for cassandra
===============================
from mongo.channel.test.stream.{  PartitionedLogCassandraSpec, AkkaCassandraPartitionedLogSpec } 

```scala
  val maxPartitionSize = 5000
  val query =
      s"""
         |SELECT * FROM DOMAIN WHERE
         |        persistence_id = ? AND
         |        partition_nr = ? AND
         |        sequence_nr >= ?
     """.stripMargin
  
  //to get Process
  val logA = (eventlog.Log[CassandraProcess] from (query, "key-a", 0, maxPartitionSize))
  
    
  //to get Observable
  val logB = (eventlog.Log[CassandraObservable] from (query, "key-a", 0, maxPartitionSize))
    
  
  //to get Akka Source
  val dName = "akka.join-dispatcher"
  val settings = ActorMaterializerSettings(system)
      .withInputBuffer(32, 64)
      .withDispatcher(dName)
      .withSupervisionStrategy(decider)
  implicit val Mat = ActorMaterializer(settings)
  implicit val dispatcher = system.dispatchers.lookup(dName)
             
  val logC = (eventlog.Log[CassandraSource] from (query, "key-a", 0, maxPartitionSize)).source
    
    
```


Join Example for cassandra
===============================
from mongo.channel.test.join.JoinCassandraSpec

```scala
  import dsl.cassandra._

  val qSensors = for { q ← select("SELECT sensor FROM {0}") } yield q

  def qTemperature(r: CRow) = for {
    _ ← select("SELECT sensor, event_time, temperature FROM {0} WHERE sensor = ?")
    q ← fk[java.lang.Long]("sensor", r.getLong("sensor"))    
  } yield q
  
  //to get Process
  val joinQuery = (Join[CassandraProcess] inner (qSensors, SENSORS, qTemperature, TEMPERATURE, KEYSPACE)) { (outer, inner) ⇒
    s"Sensor №${outer.getLong("sensor")} - time: ${inner.getLong("event_time")} temperature: ${inner.getDouble("temperature")}"
  }
  
  //to get Observable
  val joinQuery = (Join[CassandraObservable] inner (qSensors, SENSORS, qTemperature, TEMPERATURE, KEYSPACE)) { (outer, inner) ⇒
    s"Sensor №${outer.getLong("sensor")} - time: ${inner.getLong("event_time")} temperature: ${inner.getDouble("temperature")}"
  }
  
  //to get akka Source
  val dName = "akka.join-dispatcher"
    val settings = ActorMaterializerSettings(system)
      .withInputBuffer(32, 64)
      .withDispatcher(dName)
      .withSupervisionStrategy(decider)
  implicit val Mat = ActorMaterializer(settings)
  implicit val dispatcher = system.dispatchers.lookup(dName)
             
  val joinQuery = (Join[CassandraSource] inner (qSensors, SENSORS, qTemperature, TEMPERATURE, KEYSPACE)) { (outer, r) ⇒
    s"Sensor №${outer.getLong("sensor")} - time: ${inner.getLong("event_time")} temperature: ${inner.getDouble("temperature")}"
  }
    
```

Join Example for mongo
===============================
from mongo.channel.test.join.JoinMongoSpec

```scala
  import mongo._
  import dsl.mongo._
  
  val qLang = for { q ← "index" $gte 0 $lte 5 } yield q
  
  def qProg(outer: DBObject) = for { q ← "lang" $eq outer.get("index").asInstanceOf[Int] } yield q
  
  //to get Process
  val joinQuery = (Join[MongoProcess] inner (qLang, LANGS, qProg(_), PROGRAMMERS, TEST_DB)) { (outer, inner) ⇒
    s"PK:${outer.get("index")} - FK:${inner.get("lang")} - ${inner.get("name")}"
  }

  //to get Observable
  val query = (Join[MongoObservable] inner (qLang, LANGS, qProg(_), PROGRAMMERS, TEST_DB)) { (outer, inner) ⇒
    s"PK:${outer.get("index")} - [FK:${inner.get("lang")} - ${inner.get("name")}]"
  }

  //to get akka Source
  val dName = "akka.join-dispatcher"
  val settings = ActorMaterializerSettings(system)
      .withInputBuffer(32, 64)
      .withDispatcher(dName)
      .withSupervisionStrategy(decider)
  implicit val Mat = ActorMaterializer(settings)
  implicit val dispatcher = system.dispatchers.lookup(dName)
    
  val joinQuery = (Join[MongoSource] inner (qSensors, SENSORS, qTemperature, TEMPERATURE, KEYSPACE)) { (outer, inner) ⇒
    s"Sensor №${outer.getLong("sensor")} - time: ${inner.getLong("event_time")} temperature: ${inner.getDouble("temperature")}"
  }
  
```