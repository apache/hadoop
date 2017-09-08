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

package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.yarn.util.Records;

/**
 * Collector info containing collector address and collector token passed from
 * RM to AM in Allocate Response.
 */
@Public
@Evolving
public abstract class CollectorInfo {

  protected static final long DEFAULT_TIMESTAMP_VALUE = -1;

  public static CollectorInfo newInstance(String collectorAddr) {
    return newInstance(collectorAddr, null);
  }

  public static CollectorInfo newInstance(String collectorAddr, Token token) {
    CollectorInfo amCollectorInfo =
        Records.newRecord(CollectorInfo.class);
    amCollectorInfo.setCollectorAddr(collectorAddr);
    amCollectorInfo.setCollectorToken(token);
    return amCollectorInfo;
  }

  public abstract String getCollectorAddr();

  public abstract void setCollectorAddr(String addr);

  /**
   * Get delegation token for app collector which AM will use to publish
   * entities.
   * @return the delegation token for app collector.
   */
  public abstract Token getCollectorToken();

  public abstract void setCollectorToken(Token token);
}
