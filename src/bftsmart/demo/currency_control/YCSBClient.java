/**
Copyright (c) 2007-2013 Alysson Bessani, Eduardo Alchieri, Paulo Sousa, and the authors indicated in the @author tags

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package bftsmart.demo.currency_control;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import bftsmart.tom.ServiceProxy;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;

/**
 * 
 * @author Marcel Santos
 *
 */
public class YCSBClient extends DB {

	private static AtomicInteger counter = new AtomicInteger();
	private ServiceProxy proxy = null;
	private int myId;
	public String trans_id = "";

	public YCSBClient() {
	}

	@Override
	public void init() {
		Properties props = getProperties();
		int initId = Integer.valueOf((String)props.get("smart-initkey"));
		myId = initId + counter.addAndGet(1);
		proxy = new ServiceProxy(myId);
		System.out.println("YCSBKVClient. Initiated client id: " + myId);
	}

	@Override
	public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
		YCSBTransDef readTrans = new YCSBTransDef(proxy, trans_id);
		HashMap<String, byte[]> results = new HashMap<String, byte[]>();
		byte[] reply = readTrans.read(table, key, fields, results);
		System.out.println("In file YCSBClient, In function read, reply is: " + reply.toString());
		YCSBMessage replyMsg = YCSBMessage.getObject(reply);
		return replyMsg.getResult();
	}

	@Override
	public int update(String table, String key, HashMap<String, ByteIterator> values) {
		YCSBTransDef writeTrans = new YCSBTransDef(proxy, trans_id);
		Iterator<String> keys = values.keySet().iterator();
		HashMap<String, byte[]> map = new HashMap<>();
		while(keys.hasNext()) {
			String field = keys.next();
			map.put(field, values.get(field).toArray());
		}
		writeTrans.write(table, key, map);
		byte[] reply = writeTrans.commit();
		YCSBMessage replyMsg = YCSBMessage.getObject(reply);
		System.out.println("In file YCSBClient, In function write, reply is: " + reply.toString());
		return replyMsg.getResult();
	}

	@Override
	public int insert(String table, String key, HashMap<String, ByteIterator> values){
		return update(table, key, values);
	}

	@Override
	public int delete(String arg0, String arg1) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int scan(String arg0, String arg1, int arg2, Set<String> arg3, Vector<HashMap<String, ByteIterator>> arg4) {
		throw new UnsupportedOperationException();
	}

}
