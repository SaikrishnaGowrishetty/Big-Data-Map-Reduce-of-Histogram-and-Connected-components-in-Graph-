import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Graph {
def main(args: Array[ String ]) {

val conf = new SparkConf().setAppName("Graph")
val sc = new SparkContext(conf)
var graph = sc.textFile (args(0)).map( line => { val a =
line.split(",");(a(0).toLong,a(0).toLong,a.toList.map(_.toLong))})

for (i <- 1 to 5)
{
graph = graph.flatMap{ case(n,g,x)=>x.map(p=>(p,g))}
.reduceByKey((x,y)=>x min y)
.join(graph.map(d=>(d._1,d)))
.map{case(k,(e,d))=>(k,e,d._3)}
}

var res = graph.map(f=>(f._2,1)).reduceByKey(_+_)

res.collect().foreach(println)
}
}