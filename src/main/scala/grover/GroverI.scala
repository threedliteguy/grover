package grover


trait GroverI {

  def computeGraph(initialVector:Array[Double], iterations:Int, size:Int): Array[Array[Double]]

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

  def grover(a: Array[Double]): Array[Double] = {
    Array(
      -a(0) + a(1) + a(2) + a(3),
      a(0) - a(1) + a(2) + a(3),
      a(0) + a(1) - a(2) + a(3),
      a(0) + a(1) + a(2) - a(3)
    ).map(_ * 0.5d)
  }

  /*
   Note: Above is standard 2D dispersion coin.  For a 3D Grover coin, could use:
   
   See: https://iopscience.iop.org/article/10.1088/1367-2630/15/7/073041
   3D grover coin (6x6) sagemath code:

import numpy
ones=matrix( numpy.array(map(lambda x: int(x), numpy.ones((6,6)).ravel() )).reshape(6,6) )
g3=(ones - 3*matrix.identity(6))*(1/3)

[-2/3  1/3  1/3  1/3  1/3  1/3]
[ 1/3 -2/3  1/3  1/3  1/3  1/3]
[ 1/3  1/3 -2/3  1/3  1/3  1/3]
[ 1/3  1/3  1/3 -2/3  1/3  1/3]
[ 1/3  1/3  1/3  1/3 -2/3  1/3]
[ 1/3  1/3  1/3  1/3  1/3 -2/3]

  */




  val LEFT = 0
  val RIGHT = 1
  val UP = 2
  val DOWN = 3

  def mask(a: Array[Double], d: Int): Array[Double] = {
    d match {
      case LEFT => Array(a(0), 0, 0, 0)
      case RIGHT => Array(0, a(1), 0, 0)
      case UP => Array(0, 0, a(2), 0)
      case DOWN => Array(0, 0, 0, a(3))
    }
  }

  def norm(a: Array[Double]): Double = {
    a.indices.map(i => a(i) * a(i)).sum
  }


}
