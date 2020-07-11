package org.dist.firstkafka

import org.dist.common.{TestUtils, ZookeeperTestHarness}
import org.dist.simplekafka.util.ZkUtils.Broker

class MyFirstZookeeperClientTest extends ZookeeperTestHarness {
  test("should register brokers with zookeeper") {
    val myFirstZookeeperClient = new MyFirstZookeeperClient(zkClient)
    myFirstZookeeperClient.registerBroker(Broker(0, "10.11.12.100", 8080))
    myFirstZookeeperClient.registerBroker(Broker(1, "10.11.12.115", 8080))

    assert(2 == myFirstZookeeperClient.getAllBrokers().size)

  }

  test("should get notified when broker is registered") {
    val myFirstZookeeperClient = new MyFirstZookeeperClient(zkClient)
    val listener = new MyFirstBrokerChangeListener(myFirstZookeeperClient)
    myFirstZookeeperClient.subscribeBrokerChangeListener(listener)

    myFirstZookeeperClient.registerBroker(Broker(0, "10.10.10.110", 8080))
    myFirstZookeeperClient.registerBroker(Broker(1, "10.10.10.111", 8080))
    myFirstZookeeperClient.registerBroker(Broker(2, "10.10.10.112", 8080))

    TestUtils.waitUntilTrue(() => {
      listener.knownBrokers.size == 3
    }, "Waiting for all brokers to get added", 1000)
  }
}
