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
package org.apache.hadoop.hdfs.server.namenode.syncservice;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.SyncMount;
import org.apache.hadoop.hdfs.protocol.DisconnectPolicy;
import org.apache.hadoop.hdfs.protocol.MountException;

import java.net.URI;
import com.google.common.collect.Lists;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;

import java.util.ArrayList;
import java.util.List;

/**
 * Mock of the MountManager, used for testing.
 */
public class MountManagerMock extends MountManager {

  public static final String BACKUP_TEST_VALUE = "backup.test.value";
  public static final String THROW_EXCEPTION = "throwException";
  public static final String EXCEPTION_MESSAGE =
      "This is a MountManager Exception";

  public MountManagerMock() {
    // FIXME - ehiggs
    super(null);
  }

  @Override
  public String createBackup(Path localBackupPath,
      URI remoteBackupPath) throws MountException {
    return createBackup(new SyncMount(generateBackupName(), localBackupPath,
        remoteBackupPath));
  }

  @Override
  public String createBackup(SyncMount syncMount) throws MountException {
    if (syncMount.getName().equals(THROW_EXCEPTION)) {
      throw new MountException(EXCEPTION_MESSAGE);
    }

    if (syncMount.getName().equals("returnTestValue")) {
      return getConf().getTrimmed(BACKUP_TEST_VALUE);
    }
    return syncMount.getName();
  }

  @Override
  public String removeBackup(String name, DisconnectPolicy policy)
      throws MountException {
    if (name.equals(THROW_EXCEPTION)) {
      throw new MountException("This is a MountManager Exception");
    }
    return "";
  }

  @Override
  public List<SyncMount> getSyncMounts() {
    return new ArrayList<>();
  }

  @Override
  public void pause(SyncMount syncMount) throws MountException {
    if (syncMount.getName().equals(THROW_EXCEPTION)) {
      throw new MountException("This is a MountManager Exception");
    }
  }

  @Override
  public void resume(SyncMount syncMount) throws MountException {
    if (syncMount.getName().equals(THROW_EXCEPTION)) {
      throw new MountException("This is a MountManager Exception");
    }
  }

  @Override
  protected String generateBackupName() {
    return "generatedBackupName";
  }

  @Override
  public SnapshotDiffReport makeSnapshotAndPerformDiff(Path backupRoot) {
    List<SnapshotDiffReport.DiffReportEntry> diffReportList = Lists.newArrayList();
    String to = "to";
    String from = "from";
    String root = "root";
    return new SnapshotDiffReport(root, from, to, diffReportList);
  }
}
