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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.chukwa.util.DatabaseWriter;
import org.apache.hadoop.chukwa.util.PidFile;

public class Aggregator {
	private static DatabaseConfig dbc = null;

	private static Log log = LogFactory.getLog(Consolidator.class);
	private long current = 0;
    private static PidFile loader=null;

	public Aggregator() {

		dbc = new DatabaseConfig();
		Calendar now = Calendar.getInstance();
		current = now.getTimeInMillis();
	}

	public HashMap<String,String> findMacros(String query) {
		boolean add=false;
		HashMap<String,String> macroList = new HashMap<String,String>();
		String macro="";
	    for(int i=0;i<query.length();i++) {
	    	if(query.charAt(i)==']') {
	    		add=false;
	    		if(!macroList.containsKey(macro)) {
		    		String subString = computeMacro(macro);
		    		macroList.put(macro,subString);	    			
	    		}
	    		macro="";
	    	}
	    	if(add) {
	    		macro=macro+query.charAt(i);
	    	}
	    	if(query.charAt(i)=='[') {
	    		add=true;
	    	}
	    }
	    return macroList;
	}

	public String computeMacro(String macro) {
		if(macro.indexOf("avg(")==0) {
			String meta="";
			String[] table = dbc.findTableName(macro.substring(4,macro.indexOf(")")), current, current);
			try {
				String cluster = System.getProperty("CLUSTER");
				if(cluster==null) {
					cluster="unknown";
				}
				DatabaseWriter db = new DatabaseWriter(cluster);

			    String query = "select * from "+table[0]+" order by timestamp desc limit 1";
	            log.debug("Query: "+query);
	            ResultSet rs = db.query(query);
	            if(rs==null) {
	          	    throw new SQLException("Table is undefined.");
	            }
	            ResultSetMetaData rmeta = rs.getMetaData();
	            if(rs.next()) {
	            	boolean first=true;
	                for(int i=1;i<=rmeta.getColumnCount();i++) {
	                	if(!first) {
	                		meta=meta+",";
	                	}
		                if(rmeta.getColumnType(i)==java.sql.Types.VARCHAR) {
		                	meta=meta+"count("+rmeta.getColumnName(i)+") as "+rmeta.getColumnName(i);
		                	first=false;
		                } else if(rmeta.getColumnType(i)==java.sql.Types.DOUBLE || 
		                		  rmeta.getColumnType(i)==java.sql.Types.INTEGER || 
		                		  rmeta.getColumnType(i)==java.sql.Types.FLOAT) {
		                	meta=meta+"avg("+rmeta.getColumnName(i)+")";
		                	first=false;
		                } else if(rmeta.getColumnType(i)==java.sql.Types.TIMESTAMP) {
		                	// Skip the column
		                } else {
		                	meta=meta+"avg("+rmeta.getColumnName(i)+")";
		                	first=false;		                	
		                }
		            }
	            }
			} catch(SQLException ex) {
				log.error(ex);
			}
			return meta;
		} else if(macro.indexOf("now")==0) {
			return DatabaseWriter.formatTimeStamp(current);
		} else if(macro.indexOf("past_hour")==0) {
			return DatabaseWriter.formatTimeStamp(current-3600*1000L);
		}
		String[] tableList = dbc.findTableName(macro,current,current);
		return tableList[0];
	}

	public static String getContents(File aFile) {
        StringBuffer contents = new StringBuffer();    
        try {
        	BufferedReader input =  new BufferedReader(new FileReader(aFile));
        	try {
        		String line = null; //not declared within while loop
        		while (( line = input.readLine()) != null){
        			contents.append(line);
        			contents.append(System.getProperty("line.separator"));
        		}
        	} finally {
        		input.close();
        	}
        } catch (IOException ex){
        	ex.printStackTrace();
        }    
        return contents.toString();
    }

	public void process(String table, String query) {
		ResultSet rs = null;
	    long start = current;
	    long end = current;
        
		String cluster = System.getProperty("CLUSTER");
		if(cluster==null) {
			cluster="unknown";
		}
	    DatabaseWriter db = new DatabaseWriter(cluster);
			    // Find the last aggregated value from table
			    String[] tmpList = dbc.findTableName(table,start,end);
			    String timeTest = "select timestamp from "+tmpList[0]+" order by timestamp desc limit 1";
			    try {
					rs = db.query(timeTest);
				    while(rs.next()) {
				    	start=rs.getTimestamp(1).getTime();
				    	end=start;
				    }
			    } catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			    // Transform table names
                HashMap<String, String> macroList = findMacros(query);
                Iterator<String> macroKeys = macroList.keySet().iterator();
                while(macroKeys.hasNext()) {
                	String mkey = macroKeys.next();
                	log.debug("replacing:"+mkey+" with "+macroList.get(mkey));
			    	query = query.replace("["+mkey+"]", macroList.get(mkey));
                }
				log.info(query);
                db.execute(query);
            db.close();
	}

    public static void main(String[] args) {
        loader=new PidFile(System.getProperty("CLUSTER")+"Aggregator");
    	dbc = new DatabaseConfig();    	
    	String queries = Aggregator.getContents(new File(System.getenv("CHUKWA_CONF_DIR")+File.separator+"aggregator.sql"));
    	String[] query = queries.split("\n");
    	for(int i=0;i<query.length;i++) {
    		    int startOffset = query[i].indexOf("[")+1;
    		    int endOffset = query[i].indexOf("]");
    		    if(query[i].equals("")) {
    		    } else if(startOffset==-1 || endOffset==-1) {
    		    	log.error("Unable to extract table name from query:"+query[i]);
    		    } else if(query[i].indexOf("#")==0) {
    		    	log.debug("skipping: "+query[i]);
    		    } else {
    		    	String table = query[i].substring(startOffset, endOffset);
    		    	Aggregator dba = new Aggregator();
    		    	dba.process(table, query[i]);
    		    }
        }
        loader.clean();
    }

}
