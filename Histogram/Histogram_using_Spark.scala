import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Histogram {

def main ( args: Array[ String ] ) {
/* ... */
val conf = new SparkConf().setAppName("Histogram")
val sc = new SparkContext(conf)
val e = sc.textFile(args(0)).map( line => { val a = line.split(",")
("1 "+a(0),"2 "+a(1),"3"+a(2))} )
val Red = e.map(e=>(e._1,1)).reduceByKey(_+_)
val Blue = e.map(e=>(e._2,1)).reduceByKey(_+_)
val green = e.map(e=>(e._3,1)).reduceByKey(_+_)
val res = (Red++Blue++green).map { case (x,y) => x+" "+y }
res.collect().foreach(println)
sc.stop()
}
}