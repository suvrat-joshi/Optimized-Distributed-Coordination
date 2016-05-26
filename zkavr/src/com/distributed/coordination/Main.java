package com.distributed.coordination;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import com.distributed.coordination.avro.AvroOperation;
import com.distributed.coordination.zookeeper.ZKOperation;

public class Main {
	static ZKOperation zkOper = null;
	static AvroOperation avOper = null;
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Scanner reader = new Scanner(System.in);
		boolean loop = true;
		
		try {
			zkOper = new ZKOperation("localhost");
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		try {
			avOper = new AvroOperation("/Users/suvratramjoshi/Optimized Distributed Coordination/schema/sample_schema.avsc");
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		Map<String,Object> m = new HashMap<String, Object>();
		m.put("first_name", "suvrat");
		m.put("last_name", "joshi");
		m.put("address", "lalitpur");
		m.put("age", 22);
		m.put("gender", "male");
		
		Map<String,Object> t = new HashMap<String, Object>();
		t.put("first_name", "suhan");
		t.put("last_name", "shrestha");
		t.put("address", "kathmandu");
		t.put("age", 26);
		t.put("gender", "male");
		
		byte[] serializedByte = null;
		
		while(loop) {
			System.out.println("**********************************************");
			System.out.println("Select your choice");
			System.out.println("\t1. Create a znode");
			System.out.println("\t2. Read a znode");
			System.out.println("\t3. Update a znode");
			System.out.println("\t4. Delete a znode");
			System.out.println("\t5. Exit");
			System.out.println("**********************************************");
			
			try {
				int n = reader.nextInt();
				switch(n) {
				case 1:
					serializedByte = avOper.serialize(m);
					zkOper.createNode("/personalInfo", serializedByte);
					break;
				case 2:
					System.out.println(avOper.deserialize(zkOper.readNode("/personalInfo")));
					break;
				case 3:
					serializedByte = avOper.serialize(t);
					zkOper.updateNode("/personalInfo", serializedByte);
					break;
				case 4:
					zkOper.deleteNode("/personalInfo");
					break;
				case 5:
					loop = false;
					break;
				}
			} catch(Exception e) {
				System.out.println("Invalid input");
			}
		}
		
		reader.close();
			
	}
}
