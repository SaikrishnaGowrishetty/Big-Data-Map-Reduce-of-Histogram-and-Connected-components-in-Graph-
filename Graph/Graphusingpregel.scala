import org.apache.spark.graphx.{Graph => NewGraph, idd}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


import java.io._

object GraphComponents {

def main(args:Array[String])
{
 val config = new SparkConf().setAppName("Connected Components in Graph program").setMaster("local")
 val sc = new SparkContext(config)
 val RDDEdges: RDD[Edge[Long]] = sc.textFile(args(0)).map( line => { val (id, adj) = line.split(",").splitAt(1)
												(id(0).toLong,adj.toList.map(_.toLong)) } )
												.flatMap( p => p._2.map(q => (p._1, q)))
												.map(vertices => Edge(vertices._1, vertices._2, vertices._1))
val graph : NewGraph[Long, Long] = NewGraph.fromEdges(RDDEdges, "defaultProperty").mapVertices((id, _) => id)
val comp = graph.pregel(Long.MaxValue, 5)(
	(id, oldg, newg) => if(oldg<newg)oldg else newg,
	con => {
        if (con.attr < con.dstAttr) {
          Iterator((con.dstId, con.attr))
        } else if (con.srcAttr < con.attr) {
          Iterator((con.dstId, con.srcAttr))
        } else {
			Iterator.empty
		}
      },
    (x, y) => if(x<y)x else y
    )
	
	
val res = comp.vertices.map(x => (x._2, 1))
					.reduceByKey((a,b)=>(a+b))
					.collect()
					.map(y => y._1.toString +' '+ y._2.toString)
res.foreach(println)
}
}