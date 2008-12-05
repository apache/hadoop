/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.chukwa.database;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.chukwa.util.DatabaseWriter;

public class DataExpiration {
	private static DatabaseConfig dbc = null;
	private static Log log = LogFactory.getLog(DataExpiration.class);		
	public DataExpiration() {
    	if(dbc==null) {
    	    dbc = new DatabaseConfig();
    	}
    }
	public void dropTables(long start, long end) {
		String cluster = System.getProperty("CLUSTER");
		if(cluster==null) {
			cluster="unknown";
		}
		DatabaseWriter dbw = new DatabaseWriter(cluster);
		try {
			HashMap<String, String> dbNames = dbc.startWith("report.db.name.");
			Iterator<String> ki = dbNames.keySet().iterator();
			while(ki.hasNext()) {
				String name = ki.next();
				String tableName = dbNames.get(name);
				String[] tableList = dbc.findTableName(tableName, start, end);
				for(String tl : tableList) {
					log.debug("table name: "+tableList[0]);
					try {
						String[] parts = tl.split("_");
						int partition = Integer.parseInt(parts[parts.length-2]);
						String table = "";
						for(int i=0;i<parts.length-2;i++) {
							if(i!=0) {
								table=table+"_";
							}
							table=table+parts[i];
						}
						partition=partition-3;
						String dropPartition="drop table if exists "+table+"_"+partition+"_"+parts[parts.length-1];
						dbw.execute(dropPartition);
						partition--;
						dropPartition="drop table if exists "+table+"_"+partition+"_"+parts[parts.length-1];
						dbw.execute(dropPartition);
					} catch(NumberFormatException e) {
						log.error("Error in parsing table partition number, skipping table:"+tableList[0]);
					} catch(ArrayIndexOutOfBoundsException e) {
						log.debug("Skipping table:"+tableList[0]+", because it has no partition configuration.");
					}
				}
			}
		} catch(Exception e) {
			e.printStackTrace();
		}		
	}
	
	public static void usage() {
		System.out.println("DataExpiration usage:");
		System.out.println("java -jar chukwa-core.jar org.apache.hadoop.chukwa.DataExpiration <date> <time window size>");
		System.out.println("     date format: YYYY-MM-DD");
		System.out.println("     time window size: 7, 30, 91, 365");		
	}
	
	public static void main(String[] args) {
		DataExpiration de = new DataExpiration();
		long now = (new Date()).getTime();
		long start = now;
		long end = now;
		if(args.length==2) {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			try {
				start = sdf.parse(args[0]).getTime();				
				end = start + (Long.parseLong(args[1])*1440*60*1000L);
				de.dropTables(start, end);				
			} catch(Exception e) {
				usage();
			}
		} else {
			usage();
		}
    }
}
