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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.chukwa.inputtools.mdl.DataConfig;
import org.apache.hadoop.chukwa.util.DatabaseWriter;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.apache.hadoop.chukwa.util.PidFile;

import java.sql.SQLException;
import java.sql.ResultSet;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.sql.ResultSetMetaData;
import java.text.SimpleDateFormat;

public class Consolidator extends Thread {
	private DatabaseConfig dbc = new DatabaseConfig();

	private static Log log = LogFactory.getLog(Consolidator.class);
	private String table = null;
	private int[] intervals;
    private static PidFile loader=null;

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
			aYearAgo.set(2008, 1, 1, 0, 0, 0);

			long start = aYearAgo.getTimeInMillis();  //starting from 2008/01/01
			long end = start + (interval*60000);
			log.debug("start time: "+start);
			log.debug("end time: "+end);
			Calendar now = Calendar.getInstance();
			String cluster = System.getProperty("CLUSTER");
			if(cluster==null) {
				cluster="unknown";
			}
			DatabaseWriter db = new DatabaseWriter(cluster);
			String fields = null;
			String dateclause = null;
			boolean emptyPrimeKey = false;
			log.info("Consolidate for "+interval+" minutes interval.");
			
			String[] tmpTable = dbc.findTableName(this.table, start, end);
			String table = tmpTable[0];
			String sumTable="";
			if(interval==5) {
				long partition=now.getTime().getTime() / DatabaseConfig.WEEK;
				StringBuilder stringBuilder = new StringBuilder();
				stringBuilder.append(this.table);
				stringBuilder.append("_");
				stringBuilder.append(partition);
				stringBuilder.append("_week");
				table=stringBuilder.toString();
				long partition2=now.getTime().getTime() / DatabaseConfig.MONTH;
				sumTable =this.table+"_"+partition2+"_month";
			} else if(interval==30) {
				long partition=now.getTime().getTime() / DatabaseConfig.MONTH;
				table=this.table+"_"+partition+"_month";				
				long partition2=now.getTime().getTime() / DatabaseConfig.QUARTER;
				sumTable =this.table+"_"+partition2+"_month";
			} else if(interval==180) {
				long partition=now.getTime().getTime() / DatabaseConfig.QUARTER;
				table=this.table+"_"+partition+"_quarter";
				long partition2=now.getTime().getTime() / DatabaseConfig.YEAR;
				sumTable =this.table+"_"+partition2+"_month";
			} else if(interval==720) {
				long partition=now.getTime().getTime() / DatabaseConfig.YEAR;
				table=this.table+"_"+partition+"_year";
				long partition2=now.getTime().getTime() / DatabaseConfig.DECADE;
				sumTable =this.table+"_"+partition2+"_month";
			}
			// Find the most recent entry
			try {
			    String query = "select * from "+sumTable+" order by timestamp desc limit 1";
	            log.debug("Query: "+query);
	            rs = db.query(query);
	            if(rs==null) {
	          	    throw new SQLException("Table is undefined.");
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
				    String query = "select * from "+table+" order by timestamp limit 1";
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
	    	            	if(groupBy.equals("")) {
	    	            	    groupBy = " group by "+columns[i];
	    	            	} else {
		    	            	groupBy = groupBy+","+columns[i];	    	            		
	    	            	}
	    	            }
		    	    } else {
		    		    if(columnsType[i]==java.sql.Types.VARCHAR || columnsType[i]==java.sql.Types.TIMESTAMP) {
		    	            fields=fields+","+columns[i];
		    	            if(columnsType[i]==java.sql.Types.VARCHAR) {
		    	            	if(groupBy.equals("")) {
		    	            	    groupBy = " group by "+columns[i];
		    	            	} else {
		    	            	    groupBy = groupBy+","+columns[i];		    	            		
		    	            	}
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
			long partition = 0;
			String timeWindowType="week";
        	while(end < now.getTimeInMillis()-(interval*2*60000)) {
			    // Select new data sample for the given intervals
			    if(interval == 5) {
			    	timeWindowType="month";
					partition = start / DatabaseConfig.MONTH;
			    } else if(interval == 30) {
			    	timeWindowType="quarter";
					partition = start / DatabaseConfig.QUARTER;
			    } else if(interval == 180) {
			    	timeWindowType="year";
					partition = start / DatabaseConfig.YEAR;
			    } else if(interval == 720) {
			    	timeWindowType="decade";
					partition = start / DatabaseConfig.DECADE;
			    }
	            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			    String startS = formatter.format(start);
			    String endS = formatter.format(end);
			    dateclause = "Timestamp >= '"+startS+"' and Timestamp <= '"+endS+"'";
			    if(emptyPrimeKey) {
			    	groupBy = " group by FLOOR(UNIX_TIMESTAMP(TimeStamp)/"+interval*60+")";
			    }
				String query = "replace into "+this.table+"_"+partition+"_"+timeWindowType+" (select "+fields+" from "+table+" where "+dateclause+groupBy+")";
				log.debug(query);
                db.execute(query);
        		if(previousStart == start) {
        			start = start + (interval*60000);
        			end = start + (interval*60000);
            		previousStart = start;
        		}
        	}
            db.close();
		}
	}

    public static void main(String[] args) {
        DataConfig mdl = new DataConfig();
        loader=new PidFile(System.getProperty("CLUSTER")+"Consolidator");
        HashMap<String, String> tableNames = (HashMap<String, String>) mdl.startWith("consolidator.table.");
        try {
                Iterator<String> ti = (tableNames.keySet()).iterator();
                while(ti.hasNext()) {
                        String table = ti.next();
                String interval=mdl.get(table);
                table = table.substring(19);
                        log.info("Summarizing table:"+table);
                Consolidator dbc = new Consolidator(table, interval);
                dbc.run();
                }
        } catch (NullPointerException e) {
                log.error("Unable to summarize database.");
                log.error("Error:"+ExceptionUtil.getStackTrace(e));
        }
        loader.clean();
    }
}
