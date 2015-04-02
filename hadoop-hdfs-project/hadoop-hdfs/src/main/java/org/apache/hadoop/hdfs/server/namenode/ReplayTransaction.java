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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;

/**
 * The transaction that is used when replaying edit log. It diverges from
 * {@link RWTransaction} in the following:
 * <ul>
 *   <li>It does not hold the write lock of the FSDirectory as the lock has
 *   acquired by the caller.</li>
 *   <li>It makes changes directly to the DB.(TODO)</li>
 * </ul>
 */
public class ReplayTransaction extends RWTransaction {
  ReplayTransaction(FSDirectory fsd) {
    super(fsd);
  }

  ReplayTransaction begin() {
    return this;
  }

  @Override
  public void close() throws IOException {}
}
