package org.apache.hadoop.mapred.lib.db;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;

public class TestConstructQuery extends TestCase {
  
  private String[] fieldNames = new String[] { "id", "name", "value" };
  private String[] nullFieldNames = new String[] { null, null, null };
  private String expected = "INSERT INTO hadoop_output (id,name,value) VALUES (?,?,?);";
  private String nullExpected = "INSERT INTO hadoop_output VALUES (?,?,?);"; 
  
  private DBOutputFormat<DBWritable, NullWritable> format 
    = new DBOutputFormat<DBWritable, NullWritable>();
  
  public void testConstructQuery() {  
    String actual = format.constructQuery("hadoop_output", fieldNames);
    assertEquals(expected, actual);
    
    actual = format.constructQuery("hadoop_output", nullFieldNames);
    assertEquals(nullExpected, actual);
  }
  
  public void testSetOutput() throws IOException {
    JobConf job = new JobConf();
    DBOutputFormat.setOutput(job, "hadoop_output", fieldNames);
    
    DBConfiguration dbConf = new DBConfiguration(job);
    String actual = format.constructQuery(dbConf.getOutputTableName()
        , dbConf.getOutputFieldNames());
    
    assertEquals(expected, actual);
    
    job = new JobConf();
    dbConf = new DBConfiguration(job);
    DBOutputFormat.setOutput(job, "hadoop_output", nullFieldNames.length);
    assertNull(dbConf.getOutputFieldNames());
    assertEquals(nullFieldNames.length, dbConf.getOutputFieldCount());
    
    actual = format.constructQuery(dbConf.getOutputTableName()
        , new String[dbConf.getOutputFieldCount()]);
    
    assertEquals(nullExpected, actual);
  }
  
}
