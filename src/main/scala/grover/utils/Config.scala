package grover.utils

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}


object Config {

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)


  private val config =  ConfigFactory.load()

  @transient val sparkConf = new SparkConf()
    .setMaster(config.getString("application.spark.master-uri"))
    .setAppName(config.getString("application.spark.app-name"))

  val sparkContext = new SparkContext(sparkConf)


  object app {
    val appConf = config.getConfig("app")

    val systemName = appConf.getString("systemName")
    val interface = appConf.getString("interface")
    val port = appConf.getInt("port")
    val groverServiceName = appConf.getString("groverServiceName")

  }



}
