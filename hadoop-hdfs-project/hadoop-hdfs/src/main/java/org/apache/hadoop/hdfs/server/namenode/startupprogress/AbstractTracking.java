/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.server.namenode.startupprogress;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Abstract base of internal data structures used for tracking progress.  For
 * primitive long properties, {@link Long#MIN_VALUE} is used as a sentinel value
 * to indicate that the property is undefined.
 */
@InterfaceAudience.Private
abstract class AbstractTracking implements Cloneable {
  long beginTime = Long.MIN_VALUE;
  long endTime = Long.MIN_VALUE;

  /**
   * Subclass instances may call this method during cloning to copy the values of
   * all properties stored in this base class.
   * 
   * @param dest AbstractTracking destination for copying properties
   */
  protected void copy(AbstractTracking dest) {
    dest.beginTime = beginTime;
    dest.endTime = endTime;
  }
}
