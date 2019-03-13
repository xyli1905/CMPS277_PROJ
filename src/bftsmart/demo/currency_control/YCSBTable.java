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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * 
 * @author Marcel Santos
 *
 */
public class YCSBTable extends TreeMap<String, HashMap<String, byte[]>> implements Serializable {
	private static final long	serialVersionUID	= 3786544460082473686L;
	public Map<String, Map<String,byte[]>> tableMap = null;

	public YCSBTable(){
		tableMap = new TreeMap<String, Map<String, byte[]>>();
	}

	public Map<String,byte[]> addTable(String tableName, Map<String, byte[]> table) {
		return tableMap.put(tableName, table);
	}

	public byte[] addData(String tableName, String key, byte[] value) {
		Map<String,byte[]> table = tableMap.get(tableName);
		if (table == null) {
			System.out.println("In file YCSBTable, In function addTable, Master Lee does not exist!!!!!!!!!");
			return null;
		}
		byte[] ret = table.put(key, value);
		return ret;
	}

	public Map<String,byte[]> getTable(String tableName) {
		return tableMap.get(tableName);
	}

	public byte[] getEntry(String tableName, String key) {
		System.out.println("Entry key: "+ key);
		Map<String,byte[]> info= tableMap.get(tableName);
		if (info == null) {
			System.out.println("In file YCSBTable, In function getEntry, Master Lee does not exist!!!!!!!!!");
			return null;
		}
		return info.get(key);
	}
}




