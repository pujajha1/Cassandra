import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._

object cassSpark extends  App {
  val conf = new SparkConf().
    setAppName("demo")
    .setMaster("local[*]")
    .set("spark.cassandra.connection.host", "localhost")
  
  /*
  cqlsh:system> create KEYSPACE tables
          ... with replication={'class':'SimpleStrategy','replication_factor':1};
  cqlsh:tables> create table tab1(name text,age Int PRIMARY KEY );
  cqlsh:tables> select * from tab1;

 age | name
-----+------
  98 | Arya
   9 | Puja

(2 rows)
  */

  //Reading Cassandra Table
  val sp = new SparkContext(conf)
  val spark=SparkSession.builder().getOrCreate()
  /* First way of reading Cassandra Table*/
  val inp=sp.cassandraTable("tables","tab1")  // Here tables is keyspace and tab1 is the table name
  inp.collect.foreach(println)
  /* Second way of reading Cassandra Table */
  val table1 = spark.read.format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> "tab1", "keyspace" -> "tables"))
    .load()
  table1.show()
  // Writing into an existing cassandra Table
  //tab6 is already a table existing in tables keyspace
  
  table1.write.format("org.apache.spark.sql.cassandra")
    .options(Map("table" -> "tab6", "keyspace" -> "tables"))
    .save()

//Creating a new table and then writing into it
  table1.createCassandraTable("tables", "tab4", partitionKeyColumns = Some(Seq("age")), clusteringKeyColumns = Some(Seq("name")))
  //table1.write.cassandraFormat("otherwords", "test").save()

  table1.write.format("org.apache.spark.sql.cassandra")
    .options(Map("table" -> "tab4", "keyspace" -> "tables"))
    .save()



}
