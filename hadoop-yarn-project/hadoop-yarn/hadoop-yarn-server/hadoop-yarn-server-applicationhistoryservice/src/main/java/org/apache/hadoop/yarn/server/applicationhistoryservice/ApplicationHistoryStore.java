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

package org.apache.hadoop.yarn.server.applicationhistoryservice;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.service.Service;

/**
 * This class is the abstract of the storage of the application history data. It
 * is a {@link Service}, such that the implementation of this class can make use
 * of the service life cycle to initialize and cleanup the storage. Users can
 * access the storage via {@link ApplicationHistoryReader} and
 * {@link ApplicationHistoryWriter} interfaces.
 * 
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface ApplicationHistoryStore extends Service,
    ApplicationHistoryReader, ApplicationHistoryWriter {
}
