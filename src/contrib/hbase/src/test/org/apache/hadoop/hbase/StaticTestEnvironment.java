/**
 * Copyright 2007 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase;

import java.io.File;
import java.io.IOException;
import java.util.Enumeration;

import org.apache.hadoop.dfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Appender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

/**
 * Initializes test environment
 */
public class StaticTestEnvironment {
  private static final Logger LOG =
    Logger.getLogger(StaticTestEnvironment.class.getPackage().getName());

  private StaticTestEnvironment() {}                    // Not instantiable

  /** configuration parameter name for test directory */
  public static final String TEST_DIRECTORY_KEY = "test.build.data";
  
  /** set to true if "DEBUGGING" is set in the environment */
  public static boolean debugging = false;

  /**
   * Initializes parameters used in the test environment:
   * 
   * Sets the configuration parameter TEST_DIRECTORY_KEY if not already set.
   * Sets the boolean debugging if "DEBUGGING" is set in the environment.
   * If debugging is enabled, reconfigures loggin so that the root log level is
   * set to WARN and the logging level for the package is set to DEBUG.
   */
  @SuppressWarnings("unchecked")
  public static void initialize() {
    String value = null;
    
    if (System.getProperty(TEST_DIRECTORY_KEY) == null) {
      System.setProperty(TEST_DIRECTORY_KEY, new File(
          "build/contrib/hbase/test").getAbsolutePath());
    }
    
    value = System.getenv("DEBUGGING");
    if(value != null && value.equalsIgnoreCase("TRUE")) {
      debugging = true;
    }
      
    Logger rootLogger = Logger.getRootLogger();
    rootLogger.setLevel(Level.WARN);
    
    Level logLevel = Level.DEBUG;
    value = System.getenv("LOGGING_LEVEL");
    if(value != null && value.length() != 0) {
      if(value.equalsIgnoreCase("ALL")) {
        logLevel = Level.ALL;
      } else if(value.equalsIgnoreCase("DEBUG")) {
        logLevel = Level.DEBUG;
      } else if(value.equalsIgnoreCase("ERROR")) {
        logLevel = Level.ERROR;
      } else if(value.equalsIgnoreCase("FATAL")) {
        logLevel = Level.FATAL;
      } else if(value.equalsIgnoreCase("INFO")) {
        logLevel = Level.INFO;
      } else if(value.equalsIgnoreCase("OFF")) {
        logLevel = Level.OFF;
      } else if(value.equalsIgnoreCase("TRACE")) {
        logLevel = Level.TRACE;
      } else if(value.equalsIgnoreCase("WARN")) {
        logLevel = Level.WARN;
      }
    }

    ConsoleAppender consoleAppender = null;
    for(Enumeration<Appender> e = rootLogger.getAllAppenders();
    e.hasMoreElements();) {

      Appender a = e.nextElement();
      if(a instanceof ConsoleAppender) {
        consoleAppender = (ConsoleAppender)a;
        break;
      }
    }
    if(consoleAppender != null) {
      Layout layout = consoleAppender.getLayout();
      if(layout instanceof PatternLayout) {
        PatternLayout consoleLayout = (PatternLayout)layout;
        consoleLayout.setConversionPattern("%d %-5p [%t] %C{2}(%L): %m%n");
      }
    }
    LOG.setLevel(logLevel);

    if (!debugging) {
      // Turn off all the filter logging unless debug is set.
      // It is way too noisy.
      Logger.getLogger("org.apache.hadoop.hbase.filter").setLevel(Level.INFO);
    }
    // Enable mapreduce loggging for the mapreduce jobs.
    Logger.getLogger("org.apache.hadoop.mapred").setLevel(Level.DEBUG);
  }
  
  /**
   * Common method to close down a MiniDFSCluster and the associated file system
   * 
   * @param cluster
   */
  public static void shutdownDfs(MiniDFSCluster cluster) {
    if (cluster != null) {
      try {
        FileSystem fs = cluster.getFileSystem();
        if (fs != null) {
          LOG.info("Shutting down FileSystem");
          fs.close();
        }
      } catch (IOException e) {
        LOG.error("error closing file system", e);
      }

      LOG.info("Shutting down Mini DFS ");
      try {
        cluster.shutdown();
      } catch (Exception e) {
        /// Can get a java.lang.reflect.UndeclaredThrowableException thrown
        // here because of an InterruptedException. Don't let exceptions in
        // here be cause of test failure.
      }
    }
  }
}
