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

package org.apache.hadoop.chukwa.extraction.database;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.chukwa.database.DatabaseConfig;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.chukwa.extraction.engine.RecordUtil;
import org.apache.hadoop.chukwa.util.ClusterConfig;
import org.apache.hadoop.chukwa.util.DatabaseWriter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;

public class MetricDataLoader {
     private static Log log = LogFactory.getLog(MetricDataLoader.class);     
	 private static Connection conn = null;    
     private static Statement stmt = null; 
     private ResultSet rs = null; 
     private static DatabaseConfig mdlConfig = null;
     private static HashMap<String, String> normalize = null;
     private static HashMap<String, String> transformer = null;
     private static HashMap<String, Float> conversion = null;
     private static HashMap<String, String> dbTables = null;
     private HashMap<String, HashMap<String,Integer>> dbSchema = null;
     private static String newSpace="-";
     private static boolean batchMode = true;

     /** Creates a new instance of DBWriter */
    public MetricDataLoader() {        
        initEnv("");
    }
    
    public MetricDataLoader(String cluster) {
        initEnv(cluster);    	
    }
    
    private void initEnv(String cluster){
       mdlConfig = new DatabaseConfig();
       transformer = mdlConfig.startWith("metric.");
       conversion = new HashMap<String, Float>();
       normalize = mdlConfig.startWith("normalize.");
       dbTables = mdlConfig.startWith("report.db.name.");
       Iterator<?> entries = mdlConfig.iterator();
       while(entries.hasNext()) {
           String entry = entries.next().toString();
           if(entry.startsWith("conversion.")) {
               String[] metrics = entry.split("=");
               try {
            	   float convertNumber = Float.parseFloat(metrics[1]);
                   conversion.put(metrics[0],convertNumber);               
               } catch (NumberFormatException ex) {
                   log.error(metrics[0]+" is not a number.");
               }
           }
       }
       String jdbc_url = "";
       log.debug("cluster name:"+cluster);
       if(!cluster.equals("")) {
    	   ClusterConfig cc = new ClusterConfig();
    	   jdbc_url = cc.getURL(cluster);
       }
       try {
           // The newInstance() call is a work around for some
           // broken Java implementations
           String jdbcDriver = System.getenv("JDBC_DRIVER");
           Class.forName(jdbcDriver).newInstance();
           log.debug("Initialized JDBC URL: "+jdbc_url);
       } catch (Exception ex) {
           // handle the error
           log.error(ex,ex);
       }
       try {
           conn = DriverManager.getConnection(jdbc_url);
           HashMap<String, String> dbNames = mdlConfig.startWith("report.db.name.");
           Iterator<String> ki = dbNames.keySet().iterator();
           dbSchema = new HashMap<String, HashMap<String,Integer>>();
    	   DatabaseWriter dbWriter = new DatabaseWriter(cluster);
           while(ki.hasNext()) {
        	   String table = dbNames.get(ki.next().toString());
        	   String query = "select * from "+table+"_template limit 1";
        	   try {
	        	   ResultSet rs = dbWriter.query(query);
	        	   ResultSetMetaData rmeta = rs.getMetaData();
	        	   HashMap<String, Integer> tableSchema = new HashMap<String, Integer>();
	        	   for(int i=1;i<=rmeta.getColumnCount();i++) {
	        		   tableSchema.put(rmeta.getColumnName(i),rmeta.getColumnType(i));
	        	   }
	               dbSchema.put(table, tableSchema); 
        	   } catch(SQLException ex) {
        		   log.debug("table: "+table+" template does not exist, MDL will not load data for this table.");
        	   }
           }
           dbWriter.close();
       } catch (SQLException ex) {
           log.error(ex,ex);
       }       
    }
    
    public void interrupt() {
    }
    
    private String escape(String s,String c){
        
        String ns = s.trim();
        Pattern pattern=Pattern.compile(" +");
        Matcher matcher = pattern.matcher(ns);
        String s2= matcher.replaceAll(c);

        return s2;

      
    }
    
    public void process(Path source)  throws IOException, URISyntaxException, SQLException {

        System.out.println("Input file:" + source.getName());

        ChukwaConfiguration conf = new ChukwaConfiguration();
        String fsName = conf.get("writer.hdfs.filesystem");
        FileSystem fs = FileSystem.get(new URI(fsName), conf);

        SequenceFile.Reader r = 
			new SequenceFile.Reader(fs,source, conf);

        stmt = conn.createStatement(); 
        conn.setAutoCommit(false);
        
        ChukwaRecordKey key = new ChukwaRecordKey();
        ChukwaRecord record = new ChukwaRecord();
        try {
            int batch=0;
            while (r.next(key, record)) {
                    boolean isSuccessful=true;
                    String sqlTime = DatabaseWriter.formatTimeStamp(record.getTime());
                    log.debug("Timestamp: " + record.getTime());
                    log.debug("DataType: " + key.getReduceType());
                    log.debug("StreamName: " + source.getName());
		
                    String[] fields = record.getFields();
	            String table = null;
	            String[] priKeys = null;
	            HashMap<String, HashMap<String, String>> hashReport = new HashMap<String ,HashMap<String, String>>();
	            String normKey = new String();
	            String node = record.getValue("csource");
	            String recordType = key.getReduceType().toLowerCase();
	            if(dbTables.containsKey("report.db.name."+recordType)) {
	            	
	            	String[] tmp = mdlConfig.findTableName(mdlConfig.get("report.db.name."+recordType), record.getTime(), record.getTime()); 
	                table = tmp[0];
	            } else {
	            	log.debug("report.db.name."+recordType+" does not exist.");
	            	continue;
	            }
	            log.debug("table name:"+table);
	            try {
	                priKeys = mdlConfig.get("report.db.primary.key."+recordType).split(",");
	            } catch (Exception nullException) {
	            }
	            for (String field: fields) {
	            	String keyName = escape(field.toLowerCase(),newSpace);
	                String keyValue = escape(record.getValue(field).toLowerCase(),newSpace);
	                if(normalize.containsKey("normalize." + recordType + "." + keyName)) {
	                	if(normKey.equals("")) {
	                        normKey = keyName + "." + keyValue;
	                	} else {
	                		normKey = normKey + "." + keyName + "." + keyValue;
	                	}
	                }
	                String normalizedKey = "metric." + recordType + "." + normKey;
	                if(hashReport.containsKey(node)) {
	                    HashMap<String, String> tmpHash = hashReport.get(node);
	                    tmpHash.put(normalizedKey, keyValue);
	                    hashReport.put(node, tmpHash);
	                } else {
	                    HashMap<String, String> tmpHash = new HashMap<String, String>();
	                    tmpHash.put(normalizedKey, keyValue);
	                    hashReport.put(node, tmpHash);
	                }
	            }
	            for (String field: fields){                
	                String valueName=escape(field.toLowerCase(),newSpace);
	                String valueValue=escape(record.getValue(field).toLowerCase(),newSpace);
	                String normalizedKey = "metric." + recordType + "." + valueName;
	                if(!normKey.equals("")) {
	                	normalizedKey = "metric." + recordType + "." + normKey + "." + valueName;
	                }
	                if(hashReport.containsKey(node)) {
	                    HashMap<String, String> tmpHash = hashReport.get(node);
	                    tmpHash.put(normalizedKey, valueValue);
	                    hashReport.put(node, tmpHash);
	                } else {
	                    HashMap<String, String> tmpHash = new HashMap<String, String>();
	                    tmpHash.put(normalizedKey, valueValue);
	                    hashReport.put(node, tmpHash);
	                    
	                }

	            }
	            Iterator<String> i = hashReport.keySet().iterator();
	            while(i.hasNext()) {
	                long currentTimeMillis = System.currentTimeMillis();
	                Object iteratorNode = i.next();
	                HashMap<String, String> recordSet = hashReport.get(iteratorNode);
	                Iterator<String> fi = recordSet.keySet().iterator();
	                // Map any primary key that was not included in the report keyName
	                String sqlPriKeys = "";
	                try {
	                    for (String priKey : priKeys) {
	                        if(priKey.equals("timestamp")) {
	                            sqlPriKeys = sqlPriKeys + priKey + " = \"" + sqlTime +"\"";
	                        }
	                        if(!priKey.equals(priKeys[priKeys.length-1])) {
	                            sqlPriKeys = sqlPriKeys + ", ";
	                        }
	                    }
	                } catch (Exception nullException) {
	                    // ignore if primary key is empty
	                }
	                // Map the hash objects to database table columns
	                String sqlValues = "";
	                boolean firstValue=true;
	                while(fi.hasNext()) {
	                    String fieldKey = (String) fi.next();
	                    if(transformer.containsKey(fieldKey)) {
		                	if(!firstValue) {
		                		sqlValues=sqlValues+", ";
		                	}
	                    	try {
	                        	if(dbSchema.get(dbTables.get("report.db.name."+recordType)).get(transformer.get(fieldKey))== java.sql.Types.VARCHAR||
	                        			dbSchema.get(dbTables.get("report.db.name."+recordType)).get(transformer.get(fieldKey))== java.sql.Types.BLOB) {
		                        	if(conversion.containsKey("conversion."+fieldKey)) {
		                                sqlValues = sqlValues + transformer.get(fieldKey) + "=" + recordSet.get(fieldKey) + conversion.get("conversion."+fieldKey).toString();
		                        	} else {
			                            sqlValues = sqlValues + transformer.get(fieldKey) + "=\"" + recordSet.get(fieldKey)+"\"";                                                                
		                        	}
	                        	} else {
	                        		double tmp;
	                        		tmp=Double.parseDouble(recordSet.get(fieldKey).toString());
	                        		if(conversion.containsKey("conversion."+fieldKey)) {
	                        			tmp=tmp*Double.parseDouble(conversion.get("conversion."+fieldKey).toString());
	                        		}
	                        		if(Double.isNaN(tmp)) {
	                        			tmp=0;
	                        		}
	                        		sqlValues = sqlValues + transformer.get(fieldKey) + "=" + tmp;
	                        	}
	    	                    firstValue=false;	
	                        } catch (NumberFormatException ex) {
	                        	if(conversion.containsKey("conversion."+fieldKey)) {
	                                sqlValues = sqlValues + transformer.get(fieldKey) + "=" + recordSet.get(fieldKey) + conversion.get("conversion."+fieldKey).toString();
	                        	} else {
		                            sqlValues = sqlValues + transformer.get(fieldKey) + "=\"" + recordSet.get(fieldKey)+"\"";                                                                
	                        	}
	    	                    firstValue=false;
	                        }
	                    }
	                }                
	
	                String sql = null;
	                if(sqlPriKeys.length()>0) {
	                    sql = "INSERT INTO " + table + " SET " + sqlPriKeys + "," + sqlValues + 
                        " ON DUPLICATE KEY UPDATE " + sqlPriKeys + "," + sqlValues + ";";	                	
	                } else {
	                    sql = "INSERT INTO " + table + " SET " + sqlValues + 
	                          " ON DUPLICATE KEY UPDATE " + sqlValues + ";";
	                }
	                log.debug(sql);
	                if(batchMode) {
		                stmt.addBatch(sql);
		                batch++;
	                } else {
	                	stmt.execute(sql);
	                }
	                String logMsg = (isSuccessful ? "Saved" : "Error occurred in saving");
	                long latencyMillis = System.currentTimeMillis() - currentTimeMillis;
	                int latencySeconds = ((int)(latencyMillis + 500)) / 1000;
	    			if(batchMode && batch>20000) {
	    			    int[] updateCounts = stmt.executeBatch();
	    			    batch=0;
	    			}
	                log.debug(logMsg + " (" + recordType + "," + RecordUtil.getClusterName(record) +
	                       "," + record.getTime() +
	                       ") " + latencySeconds + " sec");	               
	            }

			}
			if(batchMode) {
			    int[] updateCounts = stmt.executeBatch();
			}
		} catch (SQLException ex) {
			// handle any errors
			log.error(ex, ex);
			log.error("SQLException: " + ex.getMessage());
			log.error("SQLState: " + ex.getSQLState());
			log.error("VendorError: " + ex.getErrorCode());
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException sqlEx) {
                    // ignore
                }
                rs = null;
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException sqlEx) {
                    // ignore
                }
                stmt = null;
            }
        }    	
    }
    
	public static void main(String[] args) {
		try {
			MetricDataLoader mdl = new MetricDataLoader(args[0]);
			mdl.process(new Path(args[1]));
		} catch(Exception e) {
			e.printStackTrace();
		}
    }
    
}
