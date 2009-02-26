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
package org.apache.hadoop.chukwa.inputtools.log4j;

import java.io.*;
import java.util.Enumeration;
import java.util.logging.LogManager;
import java.util.Properties;
import org.apache.hadoop.mapred.TaskLogAppender;
import org.apache.hadoop.metrics.ContextFactory;
import org.apache.hadoop.metrics.MetricsException;
import org.apache.hadoop.metrics.spi.AbstractMetricsContext;
import org.apache.hadoop.metrics.spi.OutputRecord;
import org.apache.log4j.Appender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.json.JSONException;
import org.json.JSONObject;

public class Log4JMetricsContext extends AbstractMetricsContext {

  Logger out = null; //Logger.getLogger(Log4JMetricsContext.class);
  static final Object lock = new Object();
  
  /* Configuration attribute names */
//  protected static final String FILE_NAME_PROPERTY = "fileName";
  protected static final String PERIOD_PROPERTY = "period";
  private static final String metricsLogDir = System.getProperty("hadoop.log.dir");
  private static final String user = System.getProperty("user.name");

    
  /** Creates a new instance of FileContext */
  public Log4JMetricsContext() {}
     
  public void init(String contextName, ContextFactory factory) {
    super.init(contextName, factory);
  /*      
    String fileName = getAttribute(FILE_NAME_PROPERTY);
    if (fileName != null) {
      file = new File(fileName);
    }
    */
    
    String periodStr = getAttribute(PERIOD_PROPERTY);
    if (periodStr != null) {
      int period = 0;
      try {
        period = Integer.parseInt(periodStr);
      } catch (NumberFormatException nfe) {
      }
      if (period <= 0) {
        throw new MetricsException("Invalid period: " + periodStr);
      }
      setPeriod(period);
    }
  }
  
  @Override
  protected void emitRecord(String contextName, String recordName, OutputRecord outRec)
      throws IOException {
	  if (out == null) {
		  synchronized(lock) {
			  if (out == null) {
				  java.util.Properties properties = new java.util.Properties();
				  properties.load(this.getClass().getClassLoader().getResourceAsStream("chukwa-hadoop-metrics-log4j.properties"));
				  Logger logger = Logger.getLogger(Log4JMetricsContext.class);
                  logger.setAdditivity(false);
				  PatternLayout layout = new PatternLayout(properties.getProperty("log4j.appender.chukwa."+contextName+".layout.ConversionPattern"));
				      org.apache.hadoop.chukwa.inputtools.log4j.ChukwaDailyRollingFileAppender appender =
				        new org.apache.hadoop.chukwa.inputtools.log4j.ChukwaDailyRollingFileAppender();
				  appender.setName("chukwa."+contextName);
				  appender.setLayout(layout);
				  appender.setAppend(true);
				  if(properties.getProperty("log4j.appender.chukwa."+contextName+".Dir")!=null) {
				    String logName = properties.getProperty("log4j.appender.chukwa."+contextName+".Dir")
				    +File.separator+"chukwa-"+user+"-"+contextName + "-" + System.currentTimeMillis() +".log";

					  // FIXME: Hack to make the log file readable by chukwa user. 
					  if(System.getProperty("os.name").intern()=="Linux".intern()) {
						  Runtime.getRuntime().exec("chmod 640 "+logName);
					  }
				      appender.setFile(logName);					  
				  } else {
				    appender.setFile(metricsLogDir+File.separator+"chukwa-"+user+"-"
				        +contextName + "-" + System.currentTimeMillis()+ ".log");
				  }
				  appender.activateOptions();
				  appender.setRecordType(properties.getProperty("log4j.appender.chukwa."+contextName+".recordType"));
				  appender.setChukwaClientHostname(properties.getProperty("log4j.appender.chukwa."+contextName+".chukwaClientHostname"));
				  appender.setChukwaClientPortNum(Integer.parseInt(properties.getProperty("log4j.appender.chukwa."+contextName+".chukwaClientPortNum")));
				  appender.setDatePattern(properties.getProperty("log4j.appender.chukwa."+contextName+".DatePattern"));
				  logger.addAppender(appender);
				  out = logger;
			  }
		  }
	  }
	  
	  
	JSONObject json = new JSONObject();
    try {
		json.put("contextName", contextName);
		json.put("recordName", recordName);
		json.put("chukwa_timestamp", System.currentTimeMillis());
	    for (String tagName : outRec.getTagNames()) {
            json.put(tagName, outRec.getTag(tagName));
	    }
	    for (String metricName : outRec.getMetricNames()) {
	    	json.put(metricName, outRec.getMetric(metricName));
	    }
    } catch (JSONException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}    
    out.info(json.toString());
  }

}
