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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.chukwa.util.DatabaseWriter;

public class TableCreator {
	private static DatabaseConfig dbc = null;
	private static Log log = LogFactory.getLog(TableCreator.class);		
	public TableCreator() {
    	if(dbc==null) {
    	    dbc = new DatabaseConfig();
    	}
    }
	public void createTables() {
		long now = (new Date()).getTime();
        createTables(now,now);		
	}
	public void createTables(long start, long end) {
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
				log.debug("table name: "+tableList[0]);
				try {
				    String[] parts = tableList[0].split("_");
				    int partition = Integer.parseInt(parts[parts.length-2]);
				    String table = "";
				    for(int i=0;i<parts.length-2;i++) {
				    	if(i!=0) {
				    		table=table+"_";
				    	}
				    	table=table+parts[i];
				    }
				    String query = "show create table "+table+"_template;";
				    ResultSet rs = dbw.query(query);
                    while(rs.next()) {				    
                    	log.debug("table schema: "+rs.getString(2));
                    	query=rs.getString(2);
                    	log.debug("template table name:"+table+"_template");
                    	log.debug("replacing with table name:"+table+"_"+partition+"_"+parts[parts.length-1]);
                    	log.debug("creating table: "+query);
                    	String createPartition=query.replaceFirst(table+"_template", table+"_"+partition+"_"+parts[parts.length-1]);
                    	createPartition=createPartition.replaceFirst("TABLE","TABLE IF NOT EXISTS");
                    	dbw.execute(createPartition);
                    	partition++;
                    	createPartition=query.replaceFirst(table+"_template", table+"_"+partition+"_"+parts[parts.length-1]);
                    	createPartition=createPartition.replaceFirst("TABLE","TABLE IF NOT EXISTS");
                    	dbw.execute(createPartition);
                    	partition++;
                    	createPartition=query.replaceFirst(table+"_template", table+"_"+partition+"_"+parts[parts.length-1]);
                    	createPartition=createPartition.replaceFirst("TABLE","TABLE IF NOT EXISTS");
                    	dbw.execute(createPartition);
                    }
				} catch(NumberFormatException e) {
					log.error("Error in parsing table partition number, skipping table:"+tableList[0]);
				} catch(ArrayIndexOutOfBoundsException e) {
					log.debug("Skipping table:"+tableList[0]+", because it has no partition configuration.");
				} catch(SQLException e) {
					
				}
			}
		} catch(Exception e) {
			e.printStackTrace();
		}		
	}

	public static void usage() {
		System.out.println("TableCreator usage:");
		System.out.println("java -jar chukwa-core.jar org.apache.hadoop.chukwa.TableCreator <date> <time window size>");
		System.out.println("     date format: YYYY-MM-DD");
		System.out.println("     time window size: 7, 30, 91, 365, 3650");
	}
	
	public static void main(String[] args) {
		TableCreator tc = new TableCreator();
        if(args.length==2) {
        	try {
        		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        		long start = sdf.parse(args[0]).getTime();
        		long end = start + (Long.parseLong(args[1])*1440*60*1000L);
        		tc.createTables(start, end);
        	} catch(Exception e) {
        		System.out.println("Invalid date format or time window size.");
        		e.printStackTrace();
				usage();
        	}
		} else {
			tc.createTables();
		}

    }
}
