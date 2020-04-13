import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._

object cassSpark extends  App {
  val conf = new SparkConf().
    setAppName("demo")
    .setMaster("local[*]")
    .set("spark.cassandra.connection.host", "localhost")
  val sp = new SparkContext(conf)
  val  spark=SparkSession.builder().getOrCreate()
  val inp=sp.cassandraTable("system","prepared_statements")
  inp.collect.foreach(println)
  val input=sp.parallelize(Seq(1,2,3,4))
  //val df=spark.read.format("org.apache.spark.sql.cassandra")
  //.options(Map("keyspace"->"system","table"->"local"))
  //.load()
  //df.show()



}
