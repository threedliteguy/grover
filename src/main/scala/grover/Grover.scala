package grover

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ListBuffer

case class VType(value:Array[Double], connected:Array[Boolean])

object Grover {


  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("Grover")
    val sc = new SparkContext(conf)


    val result: Array[Array[Double]] = computeGraph(Array(0d, 0d, 1d, 0d), 10, 21, sc)

    val formatted: String = getSquareGraphFormatted(result, true)

    println(formatted)


    sc.stop()

  }

  def computeGraph(initialVector:Array[Double], iterations:Int, size:Int, sc:SparkContext): Array[Array[Double]] = {

    // Set up grid
    val vs = new ListBuffer[(VertexId, VType)]
    val es = new ListBuffer[Edge[Int]]
    val LEFT = 0
    val RIGHT = 1
    val UP = 2
    val DOWN = 3

    var vcount: Long = 0
    for (y: Int <- 0 until size) {
      for (x: Int <- 0 until size) {

        val connected = Array(x > 0, x < size - 1, y < size - 1, y > 0)

        var value = Array(0d, 0d, 0d, 0d)
        if (y == Math.floor(size/2.0) && x == Math.floor(size/2.0)) value = initialVector
        val p = (vcount, new VType(value, connected))

        vs += p
        vcount += 1
      }
    }

    vs.foreach {
      case (id: VertexId, v: VType) => {
        val c = v.connected
        if (c(LEFT)) es += Edge(id, id - 1, LEFT)
        if (c(RIGHT)) es += Edge(id, id + 1, RIGHT)
        if (c(UP)) es += Edge(id, id + size, UP)
        if (c(DOWN)) es += Edge(id, id - size, DOWN)
      }
    }

    // GraphX RDDs
    val vertices: RDD[(VertexId, VType)] = sc.parallelize(vs)
    val edges: RDD[Edge[Int]] = sc.parallelize(es)
    val graph = Graph(vertices, edges)

//    println(getSquareGraphFormatted(getSquareGraph(graph, size)))
//    printSum(graph)

    // Set up Pregel functions
    val initialMsg = Array[Double]()
    val zeroMsg = Array[Double](0d, 0d, 0d, 0d)

    def vprog(vertexId: VertexId, value: VType, message: Array[Double]): VType = {
      if (message.length == 0)
        value
      else {
        //println(vertexId, message.toList)
        new VType(message, value.connected)
      }
    }

    def sendMsg(triplet: EdgeTriplet[VType, Int]): Iterator[(VertexId, Array[Double])] = {

      val v = triplet.srcAttr.value

      if (v(0) == 0 && v(1) == 0 && v(2) == 0 && v(3) == 0) {
        return Iterator()
      }

      val direction = triplet.toTuple._3

      val g: Array[Double] = grover(v)

      if (triplet.srcId == triplet.dstId) {
        // TODO Boundary reflection
      }

      val m: Array[Double] = mask(g, direction)
      if (norm(m) == 0) return Iterator()

      // zero message needed to tell pregel to run for that vertex
      Iterator((triplet.dstId, m), (triplet.srcId, zeroMsg))
    }

    def mergeMsg(m1: Array[Double], m2: Array[Double]): Array[Double] = {
      m1.indices.map(i => m1(i) + m2(i)).toArray
    }

    def grover(a: Array[Double]): Array[Double] = {
      Array(
        -a(0) + a(1) + a(2) + a(3),
        a(0) - a(1) + a(2) + a(3),
        a(0) + a(1) - a(2) + a(3),
        a(0) + a(1) + a(2) - a(3)
      ).map(_ * 0.5d)
    }

    def mask(a: Array[Double], d: Int): Array[Double] = {
      d match {
        case LEFT => Array(a(0), 0, 0, 0)
        case RIGHT => Array(0, a(1), 0, 0)
        case UP => Array(0, 0, a(2), 0)
        case DOWN => Array(0, 0, 0, a(3))
      }
    }


    val result = graph.pregel(initialMsg,
      iterations,
      EdgeDirection.Out)(
      vprog,
      sendMsg,
      mergeMsg)


    getSquareGraph(result, size)


  }


  def norm(a: Array[Double]): Double = {
    a.indices.map(i => a(i) * a(i)).sum
  }

  def getSquareGraph(g: Graph[VType, Int], size: Int): Array[Array[Double]] = {
    val result: Array[Array[Double]] = Array.ofDim[Double](size, size)

    g.vertices.collect.sortBy(_._1).foreach { p => {
      val anorm = norm(p._2.value)
      val i: Int = Math.floor(p._1 / size).toInt
      val j: Int = p._1.toInt % size
      result(i)(j) = anorm
    }
    }

    result
  }

  def getSquareGraphFormatted(a: Array[Array[Double]], newlines:Boolean): String = {
    val size = a.length
    val sb = new StringBuilder()
    sb.append("[")
    if (newlines) sb.append("\n")
    for (y: Int <- 0 until size) {
      if (y > 0) sb.append(",")
      sb.append("[")
      for (x: Int <- 0 until size) {
        if (x > 0) sb.append(", ")
        sb.append(a(y)(x) * -3000)  // hack: fixup for frontend
      }
      sb.append("]")
      if (newlines) sb.append("\n")
    }
    sb.append("]")
    if (newlines) sb.append("\n")
    sb.toString()
  }


  def printSum(g: Graph[VType, Int]): Unit = {
    val sum = g.vertices.map(p => norm(p._2.value)).sum()
    println("sum = " + sum)
  }


}