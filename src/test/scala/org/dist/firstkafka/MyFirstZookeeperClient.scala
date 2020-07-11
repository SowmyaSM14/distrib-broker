package org.dist.firstkafka

import org.I0Itec.zkclient.{IZkChildListener, ZkClient}
import org.I0Itec.zkclient.exception.{ZkInterruptedException, ZkNoNodeException}
import org.dist.simplekafka.common.JsonSerDes
import org.dist.simplekafka.util.ZkUtils
import org.dist.simplekafka.util.ZkUtils.Broker

import scala.jdk.CollectionConverters._

class MyFirstZookeeperClient(zkClient: ZkClient) {
  def getBrokerInfo(id: Int): Broker = {
    val data: String = zkClient.readData(getBrokerPath(id))
    JsonSerDes.deserialize(data.getBytes, classOf[Broker])
  }

  val BrokerIdsPath = "/brokers/ids"

  def subscribeBrokerChangeListener(listener: IZkChildListener): Option[List[String]] = {
    val result = zkClient.subscribeChildChanges(BrokerIdsPath, listener)
    Option(result).map(_.asScala.toList)
  }

  def getAllBrokers(): Set[Broker] = {
    zkClient.getChildren(BrokerIdsPath).asScala.map(brokerId => {
      val data: String = zkClient.readData(getBrokerPath(brokerId.toInt))
      JsonSerDes.deserialize(data.getBytes, classOf[Broker])
    }).toSet
  }

  def getBrokerPath(id: Int): String = {
    BrokerIdsPath + "/" + id
  }

  def createParentPath(zkClient: ZkClient, brokerPath: String): Unit = {
    val parentDir = brokerPath.substring(0, brokerPath.lastIndexOf('/'))
    if (parentDir.length != 0) {
      print("Creating parent Dir: " + parentDir)
      zkClient.createPersistent(parentDir, true)
    }
  }

  def createEphemeralPath(zkClient: ZkClient, brokerPath: String, brokerData: String): Unit = {
    try {
      zkClient.createEphemeral(brokerPath, brokerData)
    }
    catch {
      case e: ZkInterruptedException => print("Exception occurred:" + e.getCause)
      case e: ZkNoNodeException =>
        createParentPath(zkClient, brokerPath)
        zkClient.createPersistent(brokerPath, brokerData)
    }
  }

  def registerBroker(broker: ZkUtils.Broker): Unit = {
    val brokerData = JsonSerDes.serialize(broker)
    val brokerPath = getBrokerPath(broker.id)
    createEphemeralPath(zkClient, brokerPath, brokerData)
  }
}
