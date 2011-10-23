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
package org.apache.hadoop.hdfs.server.datanode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This aspect takes care about faults injected into datanode.DataXceiver 
 * class 
 */
privileged public aspect DataXceiverAspects {
  public static final Log LOG = LogFactory.getLog(DataXceiverAspects.class);

  pointcut runXceiverThread(DataXceiver xceiver) :
    execution (* run(..)) && target(xceiver);

  void around (DataXceiver xceiver) : runXceiverThread(xceiver) {
    if ("true".equals(System.getProperty("fi.enabledOOM"))) {
      LOG.info("fi.enabledOOM is enabled");
      throw new OutOfMemoryError("Pretend there's no more memory");
    } else {
    	proceed(xceiver);
    }
  }
}