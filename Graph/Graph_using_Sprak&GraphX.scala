import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.{Graph => GraphAlter, VertexId}

import java.io._

object GraphComponents {

def main(args:Array[String])
{
val config = new SparkConf().setAppName("Connected Components in Graph
program").setMaster("local")
val sc = new SparkContext(config)
val RDDEdges: RDD[Edge[Long]] = sc.textFile(args(0)).map( line => { val (vertex,
neighbours) = line.split(",").splitAt(1)(vertex(0).toLong,neighbours.toList.map(_.toLong)) } )
.flatMap( p => p._2.map(q => (p._1, q)))
.map(vertices => Edge(vertices._1,
vertices._2, vertices._1))
val graph : GraphAlter[Long, Long] = GraphAlter.fromEdges(RDDEdges,
"defaultProperty").mapVertices((id, _) => id)
val ConnComp = graph.pregel(Long.MaxValue, 5)(
(id, PrevGrp, UpdatedGrp) => math.min(PrevGrp, UpdatedGrp),
Message => {
if (Message.attr < Message.dstAttr) {
Iterator((Message.dstId, Message.attr))
} else if (Message.srcAttr < Message.attr) {
Iterator((Message.dstId, Message.srcAttr))
} else {
Iterator.empty
}
},
(p, q) => if(p<q)p else q
)


val grpcnt = ConnComp.vertices.map(Finalgraph => (Finalgraph._2, 1))
.reduceByKey((p,q)=>(p+q))
.collect()
.map(k => k._1.toString+' '+k._2.toString)
grpcnt.foreach(println)
}
}