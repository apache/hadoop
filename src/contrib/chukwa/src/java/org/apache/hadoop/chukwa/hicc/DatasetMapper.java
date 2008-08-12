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

package org.apache.hadoop.chukwa.hicc;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.sql.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DatasetMapper {
    private String jdbc;
    private static Log log = LogFactory.getLog(DatasetMapper.class);
    private HashMap<String, ArrayList<Double>> dataset;
    private List<String> labels;
	public DatasetMapper(String jdbc) {
	    this.jdbc=jdbc;
	    this.dataset = new HashMap<String, ArrayList<Double>>();
	    this.labels = new ArrayList<String>();
	}
	public void execute(String query, boolean groupBySecondColumn) {
		dataset.clear();
	    try {
	        // The newInstance() call is a work around for some
	        // broken Java implementations
	        Class.forName("com.mysql.jdbc.Driver").newInstance();
	    } catch (Exception ex) {
	        // handle the error
	    }
	    Connection conn = null;
	    Statement stmt = null;
	    ResultSet rs = null;
	    int counter = 0;
	    int size = 0;
	    labels.clear();
	    double max=0.0;
	    int labelsCount=0;
	    try {
	        conn = DriverManager.getConnection(jdbc);
	        stmt = conn.createStatement();
	        //rs = stmt.executeQuery(query);
	        if (stmt.execute(query)) {
	            rs = stmt.getResultSet();
	            ResultSetMetaData rmeta = rs.getMetaData();
	            int col=rmeta.getColumnCount();
	            int i=0;
	            java.util.ArrayList<Double> data = null;
	            HashMap<String, Integer> xAxisMap = new HashMap<String, Integer>();
	            while (rs.next()) {
	                String label = rs.getString(1);
	                if(!xAxisMap.containsKey(label)) {
	                    xAxisMap.put(label, i);
	                    labels.add(label);
	                    i++;
	                }
	                if(groupBySecondColumn) {
	                    String item = rs.getString(2);
	                    // Get the data from the row using the series column
	                    double current = rs.getDouble(3);
	                    if(current>max) {
	                        max=current;
	                    }
	                    data = dataset.get(item);
	                    if(data == null) {
	                        data = new java.util.ArrayList<Double>();
	                    }
	                    data.add(rs.getDouble(3));
	                    dataset.put(item,data);
	                } else {
	                    for(int j=2;j<=col;j++) {
	                        String item = rmeta.getColumnName(j);
	                        // Get the data from the row using the column name
	                        double current = rs.getDouble(j);
	                        if(current>max) {
	                            max=current;
	                        }
	                        data = dataset.get(item);
	                        if(data == null) {
	                            data = new java.util.ArrayList<Double>();
	                        }
	                        data.add(rs.getDouble(j));
	                        dataset.put(item,data);
	                    }
	                }
	            }
	            labelsCount=i;
	        } else {
	                log.error("query is not executed.");
	        }
	        // Now do something with the ResultSet ....
	    } catch (SQLException ex) {
	        // handle any errors
	        log.error("SQLException: " + ex.getMessage());
	        log.error("SQLState: " + ex.getSQLState());
	        log.error("VendorError: " + ex.getErrorCode());
	    } catch (Exception ex) {
	    } finally {
	        // it is a good idea to release
	        // resources in a finally{} block
	        // in reverse-order of their creation
	        // if they are no-longer needed
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
	        if (conn != null) {
	            try {
	                conn.close();
	            } catch (SQLException sqlEx) {
	                // ignore
	            }
	            conn = null;
	        }
	    }
	}
	public List<String> getXAxisMap() {
		return labels;
	}
	public HashMap<String, ArrayList<Double>> getDataset() {
		return dataset;
	}
}
