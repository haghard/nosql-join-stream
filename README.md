Nosql-join-stream
===================

Goals:
  * To demonstrate power of design based on "Abstract algebraic data type"
  * Provide support for streaming libs: [ScalazStream](https://github.com/scalaz/scalaz-stream) and [RxScala](https://github.com/ReactiveX/RxScala.git) in join process
  * Provide support for MongoDb and Cassandra with single API
  * Resource safety

The main idea for "Abstract algebraic data type" pattern was taken from this [blog post](http://io.pellucid.com/blog/abstract-algebraic-data-type)

Example for cassandra
===============================
from mongo.channel.test.join.JoinCassandraSpec

```scala
  import dsl.cassandra._

  val qSensors = for { q ← select("SELECT sensor FROM {0}") } yield q

  def qTemperature(r: CRow) = for {
    _ ← select("SELECT sensor, event_time, temperature FROM {0} WHERE sensor = ?")
    _ ← fk[java.lang.Long]("sensor", r.getLong("sensor"))
    q ← readConsistency(ConsistencyLevel.ONE)
  } yield q
  
  //to get Process
  val joinQuery = Join[CassandraProcess].join(qSensors, SENSORS, qTemperature, TEMPERATURE, KEYSPACE) { (l, r) ⇒
    s"Sensor №${l.getLong("sensor")} - time: ${r.getLong("event_time")} temperature: ${r.getDouble("temperature")}"
  }
  
  //to get Observable
  val joinQuery = Join[CassandraObservable].join(qSensors, SENSORS, qTemperature, TEMPERATURE, KEYSPACE) { (l, r) ⇒
    s"Sensor №${l.getLong("sensor")} - time: ${r.getLong("event_time")} temperature: ${r.getDouble("temperature")}"
  }
    
```

Example for mongo
===============================
from mongo.channel.test.join.JoinMongoSpec

```scala
  import mongo._
  import dsl.mongo._
  
  val qLang = for { q ← "index" $gte 0 $lte 5 } yield q
  
  def qProg(outer: DBObject) = for { q ← "lang" $eq outer.get("index").asInstanceOf[Int] } yield q
  
  //to get Process
  val joinQuery = Join[MongoProcess].join(qLang, LANGS, qProg(_), PROGRAMMERS, TEST_DB) { (l, r) ⇒
    s"PK:${l.get("index")} - FK:${r.get("lang")} - ${r.get("name")}"
  }

  //to get Observable
  val query = Join[MongoObservable].join(qLang, LANGS, qProg(_), PROGRAMMERS, TEST_DB) { (l, r) ⇒
    s"PK:${l.get("index")} - [FK:${r.get("lang")} - ${r.get("name")}]"
  }
  
```