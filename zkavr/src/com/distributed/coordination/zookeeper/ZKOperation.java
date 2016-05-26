package com.distributed.coordination.zookeeper;

import java.io.IOException;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;

public class ZKOperation {

	private ZooKeeper zkObj;
	private ZKConnect zkConnObj;

	public ZKOperation(String host) throws InterruptedException, IOException {
		zkConnObj = new ZKConnect();
		zkObj = zkConnObj.connect(host);
	}

	public void createNode(String path, byte[] data) throws KeeperException, InterruptedException {
		Stat status = zkObj.exists(path, true);

		if (status == null) {
			zkObj.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			System.out.println("Node created successfully");
		}
		else
			System.out.println("Node already exists");
	}

	public void updateNode(String path, byte[] data) throws KeeperException, InterruptedException {
		zkObj.setData(path, data, zkObj.exists(path, true).getVersion());
		System.out.println("Node updated successfully");
	}

	public byte[] readNode(String path) throws KeeperException, InterruptedException {
		Stat status = zkObj.exists(path, true);

		if (status != null) {
			byte[] b = zkObj.getData(path, null, null);
			return b;
		}
		
		return null;
	}

	public void deleteNode(String path) throws KeeperException, InterruptedException {
		zkObj.delete(path, zkObj.exists(path, true).getVersion());
		System.out.println("Node deleted successfully");
	}

	public void closeConnection() throws InterruptedException {
		zkConnObj.close();
	}
}