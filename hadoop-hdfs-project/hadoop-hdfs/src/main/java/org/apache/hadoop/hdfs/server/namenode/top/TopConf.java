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
package org.apache.hadoop.hdfs.server.namenode.top;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import com.google.common.base.Preconditions;

/**
 * This class is a common place for NNTop configuration.
 */
@InterfaceAudience.Private
public final class TopConf {

  public static final String TOP_METRICS_REGISTRATION_NAME = "topusers";
  public static final String TOP_METRICS_RECORD_NAME = "topparam";
  /**
   * A meta command representing the total number of commands
   */
  public static final String CMD_TOTAL = "total";
  /**
   * A meta user representing all users
   */
  public static String ALL_USERS = "ALL";

  /**
   * nntop reporting periods in milliseconds
   */
  public final long[] nntopReportingPeriodsMs;

  public TopConf(Configuration conf) {
    String[] periodsStr = conf.getTrimmedStrings(
        DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY,
        DFSConfigKeys.NNTOP_WINDOWS_MINUTES_DEFAULT);
    nntopReportingPeriodsMs = new long[periodsStr.length];
    for (int i = 0; i < periodsStr.length; i++) {
      nntopReportingPeriodsMs[i] = Integer.parseInt(periodsStr[i]) *
          60L * 1000L; //min to ms
    }
    for (long aPeriodMs: nntopReportingPeriodsMs) {
      Preconditions.checkArgument(aPeriodMs >= 60L * 1000L,
          "minimum reporting period is 1 min!");
    }
  }
}
