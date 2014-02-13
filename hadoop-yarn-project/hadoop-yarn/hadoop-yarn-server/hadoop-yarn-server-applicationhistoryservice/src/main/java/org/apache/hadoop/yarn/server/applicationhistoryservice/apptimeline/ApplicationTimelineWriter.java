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

package org.apache.hadoop.yarn.server.applicationhistoryservice.apptimeline;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.apptimeline.ATSEntities;
import org.apache.hadoop.yarn.api.records.apptimeline.ATSPutErrors;

import java.io.IOException;

/**
 * This interface is for storing application timeline information.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface ApplicationTimelineWriter {

  /**
   * Stores entity information to the application timeline store. Any errors
   * occurring for individual put request objects will be reported in the
   * response.
   *
   * @param data An {@link ATSEntities} object.
   * @return An {@link ATSPutErrors} object.
   * @throws IOException
   */
  ATSPutErrors put(ATSEntities data) throws IOException;

}
