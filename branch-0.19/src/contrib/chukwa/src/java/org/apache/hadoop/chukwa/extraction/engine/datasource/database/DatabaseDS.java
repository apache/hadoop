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

// Log Event Prototype 
// From event_viewer.jsp
package org.apache.hadoop.chukwa.extraction.engine.datasource.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.Record;
import org.apache.hadoop.chukwa.extraction.engine.SearchResult;
import org.apache.hadoop.chukwa.extraction.engine.datasource.DataSource;
import org.apache.hadoop.chukwa.extraction.engine.datasource.DataSourceException;
import org.apache.hadoop.chukwa.hicc.ClusterConfig;

public class DatabaseDS implements DataSource
{
		
	public SearchResult search(SearchResult result, String cluster,
			String dataSource, long t0, long t1, String filter)
			throws DataSourceException
	{
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd kk:mm:ss");
		String timeField = null;
		TreeMap<Long, List<Record>> records = result.getRecords();
		
		 if(cluster==null)
		 {
			   cluster="mithrilgold";
		  }
		
		if (dataSource.equalsIgnoreCase("MRJob"))
		{
			timeField = "LAUNCH_TIME";
		}
		else  if (dataSource.equalsIgnoreCase("HodJob"))
		{
			timeField = "StartTime";
		}
		else if (dataSource.equalsIgnoreCase("QueueInfo"))
		{
			timeField = "timestamp";
		}
		else
		{
			timeField = "timestamp";
		}
		String startS = formatter.format(t0);
	    String endS = formatter.format(t1);
	    Statement stmt = null;
	    ResultSet rs = null;
	    try
	    {
	    	String dateclause = timeField + " >= '" + startS 
	    		+ "' and " + timeField + " <= '" + endS + "'";
	    	
		       ClusterConfig cc = new ClusterConfig();
		       String jdbc = cc.getURL(cluster);
		       
			   Connection conn = DriverManager.getConnection(jdbc);
			   
			   stmt = conn.createStatement();
			   String query = "";
			   query = "select * from "+dataSource+" where "+dateclause+";";
			   rs = stmt.executeQuery(query);
			   if (stmt.execute(query)) 
			   {
			       rs = stmt.getResultSet();
			       ResultSetMetaData rmeta = rs.getMetaData();
			       int col = rmeta.getColumnCount();
			       while (rs.next()) 
			       {
			    	   ChukwaRecord event = new ChukwaRecord();
					   String cell="";
					   long timestamp = 0;
					   
					   for(int i=1;i<col;i++)
					   {
					       String value = rs.getString(i);
					       if(value!=null) 
					       {
						   cell=cell+" "+rmeta.getColumnName(i)+":"+value;
					       }
					       if(rmeta.getColumnName(i).equals(timeField)) 
					       {
					    	   timestamp = rs.getLong(i);
					    	   event.setTime(timestamp);
					       }
					   }
					   boolean isValid = false;
					   if(filter == null || filter.equals("")) 
					   {
						   isValid = true;
					   }
					   else if (cell.indexOf(filter) > 0)
					   {
						   isValid = true;
					   }
					   if (!isValid)
					   { continue; }
					   
					   event.add(Record.bodyField, cell);
					   event.add(Record.sourceField, cluster + "." + dataSource );
					   if (records.containsKey(timestamp))
					   {
						   records.get(timestamp).add(event);
					   }
					   else
					   {
						   List<Record> list = new LinkedList<Record>();
						   list.add(event);
						   records.put(event.getTime(), list);
					   }     
			       }
			   }
	    }
	    catch (SQLException e)
	    {
	    	e.printStackTrace();
	    	throw new DataSourceException(e);
	    }
	    finally 
	    {
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
		return result;
	}

	public boolean isThreadSafe()
	{
		return true;
	}

}
