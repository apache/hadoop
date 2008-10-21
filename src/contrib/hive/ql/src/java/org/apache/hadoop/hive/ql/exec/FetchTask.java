/**
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

package org.apache.hadoop.hive.ql.exec;

import java.io.Serializable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.plan.fetchWork;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

/**
 * FetchTask implementation
 **/
public class FetchTask extends Task<fetchWork> implements Serializable {
  private static final long serialVersionUID = 1L;

  static final private int MAX_ROWS  = 100;
  
  public void initialize (HiveConf conf) {
   	super.initialize(conf);
    splitNum = 0;
    currRecReader = null;
    
   	try {
       // Create a file system handle
       fs = FileSystem.get(conf);   
       serde = work.getDeserializerClass().newInstance();
       serde.initialize(null, work.getSchema());
       job = new JobConf(conf, ExecDriver.class);
       Path inputP = work.getSrcDir();
       if(!fs.exists(inputP)) {
         empty = true;
         return;
       }

       empty = true;
       FileStatus[] fStats = fs.listStatus(inputP);
       for (FileStatus fStat:fStats) {
         if (fStat.getLen() > 0) {
           empty = false;
           break;
         }
       }

       if (empty)
         return;

       FileInputFormat.setInputPaths(job, inputP);
       inputFormat = getInputFormatFromCache(work.getInputFormatClass(), job);
	     inputSplits = inputFormat.getSplits(job, 1);
	 	   mSerde = new MetadataTypedColumnsetSerDe();
       Properties mSerdeProp = new Properties();
       mSerdeProp.put(Constants.SERIALIZATION_FORMAT, "" + Utilities.tabCode);
       mSerdeProp.put(Constants.SERIALIZATION_NULL_FORMAT, "NULL");
       mSerde.initialize(null, mSerdeProp);
       totalRows = 0;
    } catch (Exception e) {
      // Bail out ungracefully - we should never hit
      // this here - but would have hit it in SemanticAnalyzer
      LOG.error(StringUtils.stringifyException(e));
      throw new RuntimeException (e);
    }
  }
  
  public int execute() {
  	assert false;
  	return 0;
  }
  
  /**
   * A cache of InputFormat instances.
   */
  private static Map<Class, InputFormat<WritableComparable, Writable>> inputFormats =
    new HashMap<Class, InputFormat<WritableComparable, Writable>>();
  
  static InputFormat<WritableComparable, Writable> getInputFormatFromCache(Class inputFormatClass, Configuration conf) throws IOException {
    if (!inputFormats.containsKey(inputFormatClass)) {
      try {
        InputFormat<WritableComparable, Writable> newInstance =
          (InputFormat<WritableComparable, Writable>)ReflectionUtils.newInstance(inputFormatClass, conf);
        inputFormats.put(inputFormatClass, newInstance);
      } catch (Exception e) {
        throw new IOException("Cannot create an instance of InputFormat class " + inputFormatClass.getName()
                               + " as specified in mapredWork!");
      }
    }
    return inputFormats.get(inputFormatClass);
  }
  
  private int splitNum;
  private FileSystem fs;  
  private RecordReader<WritableComparable, Writable> currRecReader;
  private InputSplit[] inputSplits;
  private InputFormat  inputFormat;
  private JobConf      job;
	private WritableComparable key; 
	private Writable value;
	private Deserializer  serde;
	private MetadataTypedColumnsetSerDe mSerde;
	private int totalRows;
  private boolean empty;
  
 	private RecordReader<WritableComparable, Writable> getRecordReader() throws Exception {
		if (splitNum >= inputSplits.length) 
  	  return null;
		currRecReader = inputFormat.getRecordReader(inputSplits[splitNum++], job, Reporter.NULL);
		key = currRecReader.createKey();
		value = currRecReader.createValue();
		return currRecReader;
	}
 	
  public boolean fetch(Vector<String> res) {
  	try {
      if (empty)
        return false;

      int numRows = 0;
      int rowsRet = MAX_ROWS;
      if ((work.getLimit() >= 0) && ((work.getLimit() - totalRows) < rowsRet))
        rowsRet = work.getLimit() - totalRows;
      if (rowsRet <= 0) {
        if (currRecReader != null)
          currRecReader.close();
        return false;
      }

    	while (numRows < rowsRet) {
  	    if (currRecReader == null) {
  	  	  currRecReader = getRecordReader();
  	  		if (currRecReader == null) {
            if (numRows == 0) 
            	return false;
            totalRows += numRows;
            return true;
    	    }
  	    }
      	boolean ret = currRecReader.next(key, value);
   	  	if (ret) {
   	  		Object obj = serde.deserialize(value);
   	  		res.add(((Text)mSerde.serialize(obj, serde.getObjectInspector())).toString());
   	  		numRows++;
   	  	}
   	  	else {
          currRecReader.close();
   	  		currRecReader = getRecordReader();
   	  		if (currRecReader == null) {
            if (numRows == 0) 
            	return false;
            totalRows += numRows;
            return true;
    	    }
          else {
        		key = currRecReader.createKey();
        		value = currRecReader.createValue();
          }
      	}
      }
    	totalRows += numRows;
      return true;
    }
    catch (Exception e) {
      console.printError("Failed with exception " +   e.getMessage(), "\n" + StringUtils.stringifyException(e));
      return false;
    }
  }
}
