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

package org.apache.hadoop.mapreduce.counters;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

import static org.apache.hadoop.mapreduce.MRJobConfig.*;

@InterfaceAudience.Private
public class Limits {

  private int totalCounters;
  private LimitExceededException firstViolation;

  private static boolean isInited;
  
  private static int GROUP_NAME_MAX;
  private static int COUNTER_NAME_MAX;
  private static int GROUPS_MAX;
  private static int COUNTERS_MAX;
  
  public synchronized static void init(Configuration conf) {
    if (!isInited) {
      if (conf == null) {
        conf = new JobConf();
      }
      GROUP_NAME_MAX = conf.getInt(COUNTER_GROUP_NAME_MAX_KEY,
          COUNTER_GROUP_NAME_MAX_DEFAULT);
      COUNTER_NAME_MAX = conf.getInt(COUNTER_NAME_MAX_KEY,
          COUNTER_NAME_MAX_DEFAULT);
      GROUPS_MAX = conf.getInt(COUNTER_GROUPS_MAX_KEY, COUNTER_GROUPS_MAX_DEFAULT);
      COUNTERS_MAX = conf.getInt(COUNTERS_MAX_KEY, COUNTERS_MAX_DEFAULT);
    }
    isInited = true;
  }
  
  public static int getGroupNameMax() {
    if (!isInited) {
      init(null);
    }
    return GROUP_NAME_MAX;
  }
  
  public static int getCounterNameMax() {
    if (!isInited) {
      init(null);
    }
    return COUNTER_NAME_MAX;
  }
  
  public static int getGroupsMax() {
    if (!isInited) {
      init(null);
    }
    return GROUPS_MAX;
  }
  
  public static int getCountersMax() {
    if (!isInited) {
      init(null);
    }
    return COUNTERS_MAX;
  }
  
  public static String filterName(String name, int maxLen) {
    return name.length() > maxLen ? name.substring(0, maxLen - 1) : name;
  }

  public static String filterCounterName(String name) {
    return filterName(name, getCounterNameMax());
  }

  public static String filterGroupName(String name) {
    return filterName(name, getGroupNameMax());
  }

  public synchronized void checkCounters(int size) {
    if (firstViolation != null) {
      throw new LimitExceededException(firstViolation);
    }
    int countersMax = getCountersMax();
    if (size > countersMax) {
      firstViolation = new LimitExceededException("Too many counters: "+ size +
                                                  " max="+ countersMax);
      throw firstViolation;
    }
  }

  public synchronized void incrCounters() {
    checkCounters(totalCounters + 1);
    ++totalCounters;
  }

  public synchronized void checkGroups(int size) {
    if (firstViolation != null) {
      throw new LimitExceededException(firstViolation);
    }
    int groupsMax = getGroupsMax();
    if (size > groupsMax) {
      firstViolation = new LimitExceededException("Too many counter groups: "+
                                                  size +" max="+ groupsMax);
    }
  }

  public synchronized LimitExceededException violation() {
    return firstViolation;
  }
}
