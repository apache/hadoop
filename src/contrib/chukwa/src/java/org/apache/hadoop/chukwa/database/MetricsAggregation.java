package org.apache.hadoop.chukwa.database;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MetricsAggregation
{
	 private static Log log = LogFactory.getLog(MetricsAggregation.class);
	 private static Connection conn = null;    
     private static Statement stmt = null; 
     private static ResultSet rs = null; 
     private static DatabaseConfig mdlConfig;
     
	/**
	 * @param args
	 * @throws SQLException 
	 */
	public static void main(String[] args) throws SQLException
	{
	       mdlConfig = new DatabaseConfig();
		
	       // Connect to the database
	       String jdbc_url = System.getenv("JDBC_URL_PREFIX")+mdlConfig.get("jdbc.host")+"/"+mdlConfig.get("jdbc.db");
	       if(mdlConfig.get("jdbc.user")!=null) {
	           jdbc_url = jdbc_url + "?user=" + mdlConfig.get("jdbc.user");
	           if(mdlConfig.get("jdbc.password")!=null) {
	               jdbc_url = jdbc_url + "&password=" + mdlConfig.get("jdbc.password");
	           }
	       }
	       try {
	           // The newInstance() call is a work around for some
	           // broken Java implementations
                   String jdbcDriver = System.getenv("JDBC_DRIVER");
	           Class.forName(jdbcDriver).newInstance();
	           log.info("Initialized JDBC URL: "+jdbc_url);
	       } catch (Exception ex) {
	           // handle the error
	    	   ex.printStackTrace();
	           log.error(ex,ex);
	       }
	       try {
	           conn = DriverManager.getConnection(jdbc_url);
	       } catch (SQLException ex) 
	       {
	    	   ex.printStackTrace();
	           log.error(ex,ex);
	       }      
	       
	       // get the latest timestamp for aggregation on this table
		   // Start = latest
	       
	      
	       
	       SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	        
	       long start = System.currentTimeMillis() - (1000*60*60*24);
	       long end = System.currentTimeMillis() - (1000*60*10);
	       // retrieve metadata for cluster_system_metrics
	       DatabaseConfig dbConf = new DatabaseConfig();
	       String[] tables = dbConf.findTableName("cluster_system_metrics_2018_week", start, end);
	       for(String table: tables)
	       {
	    	   System.out.println("Table to aggregate per Ts: " + table);
	    	   stmt = conn.createStatement();
	    	   rs = stmt.executeQuery("select table_ts from aggregation_admin_table where table_name=\"" 
	    			   + table + "\"");
			   if (rs.next())
			   {
				   start = rs.getLong(1);
			   }
			   else
			   {
				   start = 0;
			   }
			   
			   end = start + (1000*60*60*1); // do 1 hour aggregation max 
			   long now = System.currentTimeMillis();
			   now = now - (1000*60*10); // wait for 10 minutes
			   end = Math.min(now, end);
		     
			   // TODO REMOVE DEBUG ONLY!
			   end = now;
			   
			   System.out.println("Start Date:" + new Date(start));
			   System.out.println("End Date:" + new Date(end));
			   
		       DatabaseMetaData dbm = conn.getMetaData ();
		       rs = dbm.getColumns ( null,null,table, null);
		      	
		       List<String> cols = new ArrayList<String>();
		       while (rs.next ())
		       {
		          	String s = rs.getString (4); // 4 is column name, 5 data type etc. 
		          	System.out.println ("Name: " + s);
		          	int type = rs.getInt(5);
		          	if (type == java.sql.Types.VARCHAR)
		          	{
		          		System.out.println("Type: Varchar " + type);
		          	}
		          	else
		          	{
		          		cols.add(s);
		          		System.out.println("Type: Number " + type);
		          	}
		       }// end of while.
		       
		       // build insert into from select query
		       String initTable = table.replace("cluster_", "");
		       StringBuilder sb0 = new StringBuilder();
		       StringBuilder sb = new StringBuilder();
		       sb0.append("insert into ").append(table).append(" (");
		       sb.append(" ( select ");
		       for (int i=0;i<cols.size();i++)
		       {
		    	   sb0.append(cols.get(i));
		    	   sb.append("avg(").append(cols.get(i)).append(") ");
		    	   if (i< cols.size()-1)
		    	   {
		    		   sb0.append(",");
		    		   sb.append(",");
		    	   }
		       }
			   sb.append(" from ").append(initTable);
			   sb.append(" where timestamp between \"");
			   sb.append(formatter.format(start));
			   sb.append("\" and \"").append(formatter.format(end));
			   sb.append("\" group by timestamp  )");
			  
		        
			   // close fields
			   sb0.append(" )").append(sb);
			   System.out.println(sb0.toString());
			   
			   // run query
			   conn.setAutoCommit(false);
			   stmt = conn.createStatement();
			   stmt.execute(sb0.toString());
			   
			   // update last run
			   stmt = conn.createStatement();
			   stmt.execute("insert into aggregation_admin_table set table_ts=\"" +  formatter.format(end) +
					   "\" where table_name=\"" + table + "\"");
			   conn.commit();
	       }
	
	}

}
