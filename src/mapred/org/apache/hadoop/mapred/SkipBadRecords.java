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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * Utility class for skip bad records functionality. It contains various 
 * settings related to skipping of bad records.
 */
public class SkipBadRecords {
  
  private static final String ENABLED = "mapred.skip.mode.enabled";
  private static final String ATTEMPTS_TO_START_SKIPPING = 
    "mapred.skip.attempts.to.start.skipping";
  private static final String AUTO_INCR_MAP_PROC_COUNT = 
    "mapred.skip.map.auto.incr.proc.count";
  private static final String AUTO_INCR_REDUCE_PROC_COUNT = 
    "mapred.skip.reduce.auto.incr.proc.count";
  private static final String OUT_PATH = "mapred.skip.out.dir";
  
  /**
   * Is skipping of bad records enabled. If it is enabled 
   * the framework will try to find bad records and skip  
   * them on further attempts.
   * 
   * @param conf the configuration
   * @return <code>true</code> if skipping is enabled
   *         <code>false</code> otherwise.
   */
  public static boolean getEnabled(Configuration conf) {
    return conf.getBoolean(ENABLED, false);
  }
  
  /**
   * Set whether to enable skipping of bad records. If it is enabled 
   * the framework will try to find bad records and will 
   * try to skip them on further attempts.
   * 
   * @param conf the configuration
   * @param enabled boolean to enable/disable skipping 
   */
  public static void setEnabled(Configuration conf, boolean enabled) {
    conf.setBoolean(ENABLED, enabled);
  }

  /**
   * Get the number of Task attempts AFTER which skip mode 
   * will be kicked off. When skip mode is kicked off, the 
   * tasks reports the range of records which it will process 
   * next to the TaskTracker. So that on failures, TT knows which 
   * ones are possibly the bad records. On further executions, 
   * those are skipped.
   * 
   * @param conf the configuration
   * @return attemptsToStartSkipping no of task attempts
   */
  public static int getAttemptsToStartSkipping(Configuration conf) {
    return conf.getInt(ATTEMPTS_TO_START_SKIPPING, 2);
  }

  /**
   * Set the number of Task attempts AFTER which skip mode 
   * will be kicked off. When skip mode is kicked off, the 
   * tasks reports the range of records which it will process 
   * next to the TaskTracker. So that on failures, TT knows which 
   * ones are possibly the bad records. On further executions, 
   * those are skipped.
   * 
   * @param conf the configuration
   * @param attemptsToStartSkipping no of task attempts
   */
  public static void setAttemptsToStartSkipping(Configuration conf, 
      int attemptsToStartSkipping) {
    conf.setInt(ATTEMPTS_TO_START_SKIPPING, attemptsToStartSkipping);
  }

  /**
   * Get the flag which if set to true, 
   * Counters.Application.MAP_PROCESSED_RECORDS is incremented 
   * by MapRunner after invoking the map function. This value must be set to 
   * false for applications which process the records asynchronously 
   * or buffer the input records. For example streaming. 
   * In such cases applications should increment this counter on their own.
   * 
   * @param conf the configuration
   * @return <code>true</code> if auto increment 
   *                           Counters.Application.MAP_PROCESSED_RECORDS.
   *         <code>false</code> otherwise.
   */
  public static boolean getAutoIncrMapperProcCount(Configuration conf) {
    return conf.getBoolean(AUTO_INCR_MAP_PROC_COUNT, true);
  }
  
  /**
   * Set the flag which if set to true, 
   * Counters.Application.MAP_PROCESSED_RECORDS is incremented 
   * by MapRunner after invoking the map function. This value must be set to 
   * false for applications which process the records asynchronously 
   * or buffer the input records. For example streaming. 
   * In such cases applications should increment this counter on their own.
   * 
   * @param conf the configuration
   * @param autoIncr whether to auto increment 
   *        Counters.Application.MAP_PROCESSED_RECORDS.
   */
  public static void setAutoIncrMapperProcCount(Configuration conf, 
      boolean autoIncr) {
    conf.setBoolean(AUTO_INCR_MAP_PROC_COUNT, autoIncr);
  }
  
  /**
   * Get the flag which if set to true, 
   * Counters.Application.REDUCE_PROCESSED_RECORDS is incremented 
   * by framework after invoking the reduce function. This value must be set to 
   * false for applications which process the records asynchronously 
   * or buffer the input records. For example streaming. 
   * In such cases applications should increment this counter on their own.
   * 
   * @param conf the configuration
   * @return <code>true</code> if auto increment 
   *                           Counters.Application.REDUCE_PROCESSED_RECORDS.
   *         <code>false</code> otherwise.
   */
  public static boolean getAutoIncrReducerProcCount(Configuration conf) {
    return conf.getBoolean(AUTO_INCR_REDUCE_PROC_COUNT, true);
  }
  
  /**
   * Set the flag which if set to true, 
   * Counters.Application.REDUCE_PROCESSED_RECORDS is incremented 
   * by framework after invoking the reduce function. This value must be set to 
   * false for applications which process the records asynchronously 
   * or buffer the input records. For example streaming. 
   * In such cases applications should increment this counter on their own.
   * 
   * @param conf the configuration
   * @param autoIncr whether to auto increment 
   *        Counters.Application.REDUCE_PROCESSED_RECORDS.
   */
  public static void setAutoIncrReducerProcCount(Configuration conf, 
      boolean autoIncr) {
    conf.setBoolean(AUTO_INCR_REDUCE_PROC_COUNT, autoIncr);
  }
  
  /**
   * Get the directory to which skipped records are written. By default it is 
   * the sub directory of the output _logs directory.
   * @param conf the configuration.
   * @return path skip output directory. Null is returned if this is not set 
   * and output directory is also not set.
   */
  public static Path getSkipOutputPath(Configuration conf) {
    String name =  conf.get(OUT_PATH);
    if(name!=null) {
      return new Path(name);
    }
    Path outPath = FileOutputFormat.getOutputPath(new JobConf(conf));
    return outPath==null ? null : new Path(outPath, 
        "_logs"+Path.SEPARATOR+"skip");
  }
  
  /**
   * Set the directory to which skipped records are written. By default it is 
   * the sub directory of the output _logs directory.
   * @param conf the configuration.
   * @param path skip output directory path
   */
  public static void setSkipOutputPath(JobConf conf, Path path) {
    conf.set(OUT_PATH, path.toString());
  }
  
}
