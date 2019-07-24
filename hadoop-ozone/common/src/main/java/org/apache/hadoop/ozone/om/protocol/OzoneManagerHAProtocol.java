/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.protocol;

import java.io.IOException;

/**
 * Protocol to talk to OM HA. These methods are needed only called from
 * OmRequestHandler.
 */
public interface OzoneManagerHAProtocol {

  /**
   * Store the snapshot index i.e. the raft log index, corresponding to the
   * last transaction applied to the OM RocksDB, in OM metadata dir on disk.
   * @param flush flush the OM DB to disk if true
   * @return the snapshot index
   * @throws IOException
   */
  long saveRatisSnapshot(boolean flush) throws IOException;

}
