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
import static org.apache.hadoop.mapreduce.MRJobConfig.*;

@InterfaceAudience.Private
public class Limits {

  static final Configuration conf = new Configuration();
  public static final int GROUP_NAME_MAX =
      conf.getInt(COUNTER_GROUP_NAME_MAX_KEY, COUNTER_GROUP_NAME_MAX_DEFAULT);
  public static final int COUNTER_NAME_MAX =
      conf.getInt(COUNTER_NAME_MAX_KEY, COUNTER_NAME_MAX_DEFAULT);
  public static final int GROUPS_MAX =
      conf.getInt(COUNTER_GROUPS_MAX_KEY, COUNTER_GROUPS_MAX_DEFAULT);
  public static final int COUNTERS_MAX =
      conf.getInt(COUNTERS_MAX_KEY, COUNTERS_MAX_DEFAULT);

  private int totalCounters;
  private LimitExceededException firstViolation;

  public static String filterName(String name, int maxLen) {
    return name.length() > maxLen ? name.substring(0, maxLen - 1) : name;
  }

  public String filterCounterName(String name) {
    return filterName(name, COUNTER_NAME_MAX);
  }

  public String filterGroupName(String name) {
    return filterName(name, GROUP_NAME_MAX);
  }

  public synchronized void checkCounters(int size) {
    if (firstViolation != null) {
      throw new LimitExceededException(firstViolation);
    }
    if (size > COUNTERS_MAX) {
      firstViolation = new LimitExceededException("Too many counters: "+ size +
                                                  " max="+ COUNTERS_MAX);
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
    if (size > GROUPS_MAX) {
      firstViolation = new LimitExceededException("Too many counter groups: "+
                                                  size +" max="+ GROUPS_MAX);
    }
  }

  public synchronized LimitExceededException violation() {
    return firstViolation;
  }
}
