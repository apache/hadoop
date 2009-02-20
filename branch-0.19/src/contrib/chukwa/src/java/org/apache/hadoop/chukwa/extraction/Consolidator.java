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

package org.apache.hadoop.chukwa.extraction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.chukwa.inputtools.mdl.DataConfig;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.apache.hadoop.chukwa.util.DatabaseWriter;

import java.sql.*;
import java.util.Calendar;
import java.sql.ResultSetMetaData;
import java.text.SimpleDateFormat;

public class Consolidator extends Thread {
	private Log log = LogFactory.getLog(Consolidator.class);
	private String table = null;
	private String jdbc = null;
	private int[] intervals;
	public Consolidator(String table, String intervalString) {
		super(table);
		try {
			int i=0;
			String[] temp = intervalString.split("\\,");
			intervals = new int[temp.length];
			for(String s: temp) {
			    intervals[i]=Integer.parseInt(s);
			    i++;
			}
			this.table = table;
		} catch (NumberFormatException ex) {
			log.error("Unable to parse summary interval");
		}		
	}
	public void run() {
		ResultSet rs = null;
		String[] columns;
		int[] columnsType;
        String groupBy = "";
        
		for(int interval : intervals) {
			// Start reducing from beginning of time;
			Calendar aYearAgo = Calendar.getInstance();
			aYearAgo.set(2008, 12, 30, 0, 0, 0);

			long start = aYearAgo.getTimeInMillis();  //starting from 2008/01/01
			long end = start + (interval*60000);
		    log.debug("start time: "+start);
		    log.debug("end time: "+end);
			Calendar now = Calendar.getInstance();
			DatabaseWriter db = new DatabaseWriter();
			String fields = null;
			String dateclause = null;
			boolean emptyPrimeKey = false;
			log.debug("Consolidate for "+interval+" minutes interval.");
			String table = this.table+"_"+interval;
			// Find the most recent entry
			try {
			    String query = "select * from "+table+" order by timestamp desc limit 1";
	            log.debug("Query: "+query);
	            rs = db.query(query);
	            if(rs==null) {
	          	    throw new SQLException("Table undefined.");
	            }
	            ResultSetMetaData rmeta = rs.getMetaData();
	            boolean empty=true;
	            if(rs.next()) {
	                for(int i=1;i<=rmeta.getColumnCount();i++) {
		                if(rmeta.getColumnName(i).toLowerCase().equals("timestamp")) {
		            	    start = rs.getTimestamp(i).getTime();
		                }
	                }
	                empty=false;
	            }
	            if(empty) {
	              	throw new SQLException("Table is empty.");
	            }
                end = start + (interval*60000);
			} catch (SQLException ex) {
			    try {
				    String query = "select * from "+this.table+" order by timestamp limit 1";
		            log.debug("Query: "+query);
	                rs = db.query(query);
	                if(rs.next()) {
	    	            ResultSetMetaData rmeta = rs.getMetaData();
	    	            for(int i=1;i<=rmeta.getColumnCount();i++) {
	    	                if(rmeta.getColumnName(i).toLowerCase().equals("timestamp")) {
	    	                	start = rs.getTimestamp(i).getTime();
	    	                }
	    	            }
				    }
                    end = start + (interval*60000);
				} catch(SQLException ex2) {
				    log.error("Unable to determine starting point in table: "+this.table);
					log.error("SQL Error:"+ExceptionUtil.getStackTrace(ex2));
					return;
				}
			}
			try {
                ResultSetMetaData rmeta = rs.getMetaData();
                int col = rmeta.getColumnCount();
                columns = new String[col];
                columnsType = new int[col];
                for(int i=1;i<=col;i++) {
            	    columns[i-1]=rmeta.getColumnName(i);
              	    columnsType[i-1]=rmeta.getColumnType(i);
                }

		        for(int i=0;i<columns.length;i++) {
		    	    if(i==0) {
		    		    fields=columns[i];
	    	            if(columnsType[i]==java.sql.Types.VARCHAR) {
	    	            	groupBy = " group by "+columns[i];
	    	            }
		    	    } else {
		    		    if(columnsType[i]==java.sql.Types.VARCHAR || columnsType[i]==java.sql.Types.TIMESTAMP) {
		    	            fields=fields+","+columns[i];
		    	            if(columnsType[i]==java.sql.Types.VARCHAR) {
		    	            	groupBy = " group by "+columns[i];
		    	            }
		    		    } else {
		    	            fields=fields+",AVG("+columns[i]+") as "+columns[i];
		    		    }
		    	    }
		        }
			} catch(SQLException ex) {
			  	log.error("SQL Error:"+ExceptionUtil.getStackTrace(ex));
			  	return;
			}
            if(groupBy.equals("")) {
            	emptyPrimeKey = true;
            }
			long previousStart = start;
        	while(end < now.getTimeInMillis()-(interval*2*60000)) {
			    // Select new data sample for the given intervals
			    if(interval == 5) {
				    table=this.table;
			    } else if(interval == 30) {
				    table=this.table+"_5";				
			    } else if(interval == 120) {
				    table=this.table+"_30";
			    }
	            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			    String startS = formatter.format(start);
			    String endS = formatter.format(end);
			    dateclause = "Timestamp >= '"+startS+"' and Timestamp <= '"+endS+"'";
			    if(emptyPrimeKey) {
			    	groupBy = "group by "+dateclause;
			    }
				String query = "insert ignore into "+this.table+"_"+interval+" (select "+fields+" from "+table+" where "+dateclause+groupBy+")";
				log.debug(query);
                db.execute(query);
                db.close();
        		if(previousStart == start) {
        			start = start + (interval*60000);
        			end = start + (interval*60000);
            		previousStart = start;
        		}
        	}
		}
	}
}
