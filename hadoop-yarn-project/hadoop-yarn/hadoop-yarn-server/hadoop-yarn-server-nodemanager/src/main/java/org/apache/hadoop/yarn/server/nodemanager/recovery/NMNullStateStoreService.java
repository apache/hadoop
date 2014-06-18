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

package org.apache.hadoop.yarn.server.nodemanager.recovery;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.proto.YarnProtos.LocalResourceProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.DeletionServiceDeleteTaskProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto;

// The state store to use when state isn't being stored
public class NMNullStateStoreService extends NMStateStoreService {

  public NMNullStateStoreService() {
    super(NMNullStateStoreService.class.getName());
  }

  @Override
  public boolean canRecover() {
    return false;
  }

  @Override
  public RecoveredLocalizationState loadLocalizationState()
      throws IOException {
    throw new UnsupportedOperationException(
        "Recovery not supported by this state store");
  }

  @Override
  public void startResourceLocalization(String user, ApplicationId appId,
      LocalResourceProto proto, Path localPath) throws IOException {
  }

  @Override
  public void finishResourceLocalization(String user, ApplicationId appId,
      LocalizedResourceProto proto) throws IOException {
  }

  @Override
  public void removeLocalizedResource(String user, ApplicationId appId,
      Path localPath) throws IOException {
  }

  @Override
  public RecoveredDeletionServiceState loadDeletionServiceState()
      throws IOException {
    throw new UnsupportedOperationException(
        "Recovery not supported by this state store");
  }

  @Override
  public void storeDeletionTask(int taskId,
      DeletionServiceDeleteTaskProto taskProto) throws IOException {
  }

  @Override
  public void removeDeletionTask(int taskId) throws IOException {
  }

  @Override
  protected void initStorage(Configuration conf) throws IOException {
  }

  @Override
  protected void startStorage() throws IOException {
  }

  @Override
  protected void closeStorage() throws IOException {
  }
}
