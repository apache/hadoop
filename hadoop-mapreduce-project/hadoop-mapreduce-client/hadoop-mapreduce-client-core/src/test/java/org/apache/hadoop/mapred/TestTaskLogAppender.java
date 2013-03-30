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


package org.apache.hadoop.mapred;

import java.io.StringWriter;
import java.io.Writer;

import org.apache.log4j.Category;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.Priority;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestTaskLogAppender {
/**
 * test TaskLogAppender 
 */
  @SuppressWarnings("deprecation")
  @Test (timeout=5000)
  public void testTaskLogAppender(){
    TaskLogAppender appender= new TaskLogAppender();
    
    System.setProperty(TaskLogAppender.TASKID_PROPERTY,"attempt_01_02_m03_04_001");
    System.setProperty(TaskLogAppender.LOGSIZE_PROPERTY, "1003");
    appender.activateOptions();
    assertEquals(appender.getTaskId(), "attempt_01_02_m03_04_001");
    assertEquals(appender.getTotalLogFileSize(),1000);
    assertEquals(appender.getIsCleanup(),false);
    
    // test writer   
    Writer writer= new StringWriter();
    appender.setWriter(writer);
    Layout layout =  new PatternLayout("%-5p [%t]: %m%n");
    appender.setLayout(layout);
    Category logger= Logger.getLogger(getClass().getName());
    LoggingEvent event = new LoggingEvent("fqnOfCategoryClass", logger, Priority.INFO, "message", new Throwable());
    appender.append(event);
    appender.flush() ;
    appender.close();
    assertTrue(writer.toString().length()>0);
    
    // test cleanup should not changed 
    appender= new TaskLogAppender();
    appender.setIsCleanup(true);
    appender.activateOptions();
    assertEquals(appender.getIsCleanup(),true);

  
  }
  
}
