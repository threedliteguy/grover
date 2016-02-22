package grover.service


import grover.Grover
import grover.model.{GroverRequest, GroverResult}
import grover.utils.Config


import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future



trait GroverService {

  def compute(groverRequest:GroverRequest): Future[Option[GroverResult]]

}

object GroverServiceI extends GroverService {


  override def compute(groverRequest:GroverRequest): Future[Option[GroverResult]] = {
    Future {
//      Some(new GroverResult("[[1,2,3],[1,3,5],[1,4,8]]"))
      val array = groverRequest.initialVector.split(",").map(_.toDouble)
      Some(new GroverResult(Grover.getSquareGraphFormatted(Grover.computeGraph(array, groverRequest.iterations, groverRequest.size, Config.sparkContext),false)))
    }
  }

}
