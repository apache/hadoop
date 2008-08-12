package org.apache.hadoop.chukwa.extraction.database;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.Record;

public class DatabaseHelper
{
	private static final String databaseRecordType = "Database/Database_";	
	static final String sqlField = "sql";	
	private String table = null;
	private HashMap<String, Object> pairs = new HashMap<String, Object>();
	private ArrayList<String> array = null;
	
	public DatabaseHelper()
	{
	    array = new ArrayList<String>();
	}

	public DatabaseHelper(String table) {
		this.table = table;
	}

	public void add(HashMap<String, Object> pairs) {
		Iterator<String> ki = pairs.keySet().iterator();
		while(ki.hasNext()) {
			String keyName = ki.next();
			Object value = pairs.get(keyName);
			if(value.getClass().getName().equals("String")) {
				array.add(keyName+"=\""+value+"\"");
			} else {
				array.add(keyName+"="+value);
			}
		}
	}
	
	public void add(long time, String key, String value) {
        array.add("timestamp="+time+","+key+"=\""+value+"\"");
	}

	public void add(long time, String key, double value) {
			array.add("timestamp="+time+","+key+"="+value);
	}

	
	public String toString() {
		StringBuffer batch = new StringBuffer();
		StringBuffer keyValues = new StringBuffer();
		for(int i=0;i<array.size();i++) {
			keyValues.append(array.get(i));
			batch.append("INSERT INTO ");
			batch.append(table);
			batch.append(" SET ");
			batch.append(keyValues);
			batch.append(" ON DUPLICATE KEY UPDATE ");
			batch.append(keyValues);
		}		
        return batch.toString();
	}
	public ChukwaRecord buildChukwaRecord()	{
		ChukwaRecord chukwaRecord = new ChukwaRecord();
		StringBuilder queries = new StringBuilder();
		
		chukwaRecord.add(sqlField, this.toString());
		chukwaRecord.add(Record.destinationField, DatabaseHelper.databaseRecordType + table + ".evt");
		chukwaRecord.add(Record.dataSourceField, DatabaseHelper.databaseRecordType + table );
		return chukwaRecord;
	}
}
