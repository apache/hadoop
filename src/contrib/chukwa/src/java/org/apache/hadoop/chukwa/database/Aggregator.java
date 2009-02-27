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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.sql.DatabaseMetaData;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.chukwa.inputtools.mdl.DataConfig;
import org.apache.hadoop.chukwa.util.DatabaseWriter;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.apache.hadoop.chukwa.util.PidFile;

public class Aggregator {
	private static DatabaseConfig dbc = null;

	private static Log log = LogFactory.getLog(Aggregator.class);
	private String table = null;
	private String jdbc = null;
	private int[] intervals;
	private long current = 0;
    private static DatabaseWriter db = null;
    public Aggregator() {
		dbc = new DatabaseConfig();
		Calendar now = Calendar.getInstance();
		current = now.getTimeInMillis();
	}

	public HashMap<String,String> findMacros(String query) throws SQLException {
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

	public String computeMacro(String macro) throws SQLException {
		Pattern p = Pattern.compile("past_(.*)_minutes");
		Matcher matcher = p.matcher(macro);
		if(macro.indexOf("avg(")==0 || macro.indexOf("group_avg(")==0 || macro.indexOf("sum(")==0) {
			String meta="";
			String[] table = dbc.findTableName(macro.substring(macro.indexOf("(")+1,macro.indexOf(")")), current, current);
			try {
				String cluster = System.getProperty("CLUSTER");
				if(cluster==null) {
					cluster="unknown";
				}
                DatabaseMetaData dbMetaData = db.getConnection().getMetaData();
	            ResultSet rs = dbMetaData.getColumns ( null,null,table[0], null);
	            boolean first=true;
	            while(rs.next()) {
	            	if(!first) {
	            		meta = meta+",";
	            	}
	            	String name = rs.getString(4);
	            	int type = rs.getInt(5);
	            	if(type==java.sql.Types.VARCHAR) {
	            		if(macro.indexOf("group_avg(")<0) {
	            			meta=meta+"count("+name+") as "+name;
	            		} else {
	            			meta=meta+name;
	            		}
		            	first=false;
	            	} else if(type==java.sql.Types.DOUBLE ||
	            			  type==java.sql.Types.FLOAT ||
	            			  type==java.sql.Types.INTEGER) {
	            		if(macro.indexOf("sum(")==0) {
	            		    meta=meta+"sum("+name+")";	            			
	            		} else {
	            		    meta=meta+"avg("+name+")";
	            		}
		            	first=false;
	            	} else if(type==java.sql.Types.TIMESTAMP) {
	            		// Skip the column
	            	} else {
	            		if(macro.indexOf("sum(")==0) {
	            		    meta=meta+"SUM("+name+")";
	            		} else {
		            		meta=meta+"AVG("+name+")";	            			
	            		}
		            	first=false;
	            	}
	            }
	            if(first) {
	          	    throw new SQLException("Table is undefined.");
	            }
			} catch(SQLException ex) {
				throw new SQLException("Table does not exist:"+ table[0]);
			}
			return meta;
		} else if(macro.indexOf("now")==0) {
			SimpleDateFormat sdf = new SimpleDateFormat();
			return DatabaseWriter.formatTimeStamp(current);
		} else if(matcher.find()) {
			int period = Integer.parseInt(matcher.group(1));
			long timestamp = current - (current % (period*60*1000L)) - (period*60*1000L);
			return DatabaseWriter.formatTimeStamp(timestamp);
		} else if(macro.indexOf("past_hour")==0) {
			return DatabaseWriter.formatTimeStamp(current-3600*1000L);
		} else if(macro.endsWith("_week")) {
			long partition = current / DatabaseConfig.WEEK;
			if(partition<=0) {
				partition=1;
			}
			String[] buffers = macro.split("_");
			StringBuffer tableName = new StringBuffer();
			for(int i=0;i<buffers.length-1;i++) {
				tableName.append(buffers[i]);
				tableName.append("_");
			}
			tableName.append(partition);
			tableName.append("_week");
			return tableName.toString();
		} else if(macro.endsWith("_month")) {
			long partition = current / DatabaseConfig.MONTH;
			if(partition<=0) {
				partition=1;
			}
			String[] buffers = macro.split("_");
			StringBuffer tableName = new StringBuffer();
			for(int i=0;i<buffers.length-1;i++) {
				tableName.append(buffers[i]);
				tableName.append("_");
			}
			tableName.append(partition);
			tableName.append("_month");
			return tableName.toString();
		} else if(macro.endsWith("_quarter")) {
			long partition = current / DatabaseConfig.QUARTER;
			if(partition<=0) {
				partition=1;
			}
			String[] buffers = macro.split("_");
			StringBuffer tableName = new StringBuffer();
			for(int i=0;i<buffers.length-1;i++) {
				tableName.append(buffers[i]);
				tableName.append("_");
			}
			tableName.append(partition);
			tableName.append("_quarter");
			return tableName.toString();
		} else if(macro.endsWith("_year")) {
			long partition = current / DatabaseConfig.YEAR;
			if(partition<=0) {
				partition=1;
			}
			String[] buffers = macro.split("_");
			StringBuffer tableName = new StringBuffer();
			for(int i=0;i<buffers.length-1;i++) {
				tableName.append(buffers[i]);
				tableName.append("_");
			}
			tableName.append(partition);
			tableName.append("_year");
			return tableName.toString();
		} else if(macro.endsWith("_decade")) {
			long partition = current / DatabaseConfig.DECADE;
			if(partition<=0) {
				partition=1;
			}
			String[] buffers = macro.split("_");
			StringBuffer tableName = new StringBuffer();
			for(int i=0;i<buffers.length-1;i++) {
				tableName.append(buffers[i]);
				tableName.append("_");
			}
			tableName.append(partition);
			tableName.append("_decade");
			return tableName.toString();
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

	public void process(String query) {
		ResultSet rs = null;
		String[] columns;
		int[] columnsType;
        String groupBy = "";
	    long start = current;
	    long end = current;
        

		try {
            HashMap<String, String> macroList = findMacros(query);
            Iterator<String> macroKeys = macroList.keySet().iterator();
            while(macroKeys.hasNext()) {
        	    String mkey = macroKeys.next();
        	    log.debug("replacing:"+mkey+" with "+macroList.get(mkey));
	    	    query = query.replace("["+mkey+"]", macroList.get(mkey));
            }
            db.execute(query);
		} catch(SQLException e) {
		    log.error(query);
			log.error(e.getMessage());
		}
	}

    public static void main(String[] args) {
        log.info("Aggregator started.");
    	dbc = new DatabaseConfig();
		String cluster = System.getProperty("CLUSTER");
		if(cluster==null) {
			cluster="unknown";
		}
    	db = new DatabaseWriter(cluster);
    	String queries = Aggregator.getContents(new File(System.getenv("CHUKWA_CONF_DIR")+File.separator+"aggregator.sql"));
    	String[] query = queries.split("\n");
    	for(int i=0;i<query.length;i++) {
    		    if(query[i].equals("")) {
    		    } else if(query[i].indexOf("#")==0) {
    		    	log.debug("skipping: "+query[i]);
    		    } else {
    		    	Aggregator dba = new Aggregator();
    		    	dba.process(query[i]);
    		    }
        }
        db.close();
    	log.info("Aggregator finished.");
    }

}
