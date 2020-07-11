package org.dist.firstkafka

import java.util

import org.I0Itec.zkclient.IZkChildListener
import org.dist.simplekafka.common.Logging
import org.dist.simplekafka.util.ZkUtils.Broker

import scala.jdk.CollectionConverters._

class MyFirstBrokerChangeListener(myFirstZookeeperClient: MyFirstZookeeperClient) extends IZkChildListener with Logging {
  var knownBrokers: Set[Broker] = Set()

  override def handleChildChange(parentPath: String, currentChildren: util.List[String]): Unit = {
    info("Broker change listener fired for path %s with children %s".format(parentPath, currentChildren.asScala.mkString(",")))
    try {
      val currentBrokerIds = currentChildren.asScala.map(_.toInt).toSet
      val newBrokerIds = currentBrokerIds -- knownBrokerIds
      val newBrokers = newBrokerIds.map(b => myFirstZookeeperClient.getBrokerInfo(b))

      newBrokers.foreach(b => knownBrokers += b)
    }
    catch {
      case e: Throwable => error("Error while handling broker changes", e)
    }
  }

  private def knownBrokerIds = {
    knownBrokers.map(broker => broker.id)
  }
}
