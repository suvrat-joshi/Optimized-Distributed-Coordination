package com.distributed.coordination.zookeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

public class ZKConnect {

	private ZooKeeper zkObj;
	final CountDownLatch connectedSignal = new CountDownLatch(1);

	public ZooKeeper connect(String host) throws IOException, InterruptedException {
		zkObj = new ZooKeeper(host, 5000, new Watcher() {
			public void process(WatchedEvent we) {
				if (we.getState() == KeeperState.SyncConnected) {
					connectedSignal.countDown();
				}
			}
		});

		connectedSignal.await();
		return zkObj;
	}

	public void close() throws InterruptedException {
		zkObj.close();
	}
}