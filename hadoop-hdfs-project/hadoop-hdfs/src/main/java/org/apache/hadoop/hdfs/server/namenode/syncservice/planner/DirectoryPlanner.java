/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode.syncservice.planner;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.hdfs.protocol.SyncMount;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectorySnapshottableFeature;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.server.namenode.syncservice.SyncServiceFileFilter;
import org.apache.hadoop.hdfs.server.protocol.SyncTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import static org.apache.hadoop.hdfs.protocol.HdfsConstants.DOT_SNAPSHOT_DIR;
import static org.apache.hadoop.hdfs.server.namenode.syncservice.RemoteSyncURICreator.createRemotePathFromAbsolutePath;

public class DirectoryPlanner {

  private static final Logger LOG =
      LoggerFactory.getLogger(DirectoryPlanner.class);

  private FilePlanner filePlanner;
  private FSDirectory fsDirectory;
  private SyncServiceFileFilter syncServiceFileFilter;

  public DirectoryPlanner(FilePlanner filePlanner, FSDirectory fsDirectory,
      SyncServiceFileFilter syncServiceFileFilter) {
    this.filePlanner = filePlanner;
    this.fsDirectory = fsDirectory;
    this.syncServiceFileFilter = syncServiceFileFilter;
  }

  static File convertPathToAbsoluteFile(byte[] path,
      Path localBackupPath) {
    String sourcePath = new String(path);
    if (sourcePath.equals(".")) {
      return new File(localBackupPath.toString());
    } else {
      return new File(localBackupPath.toString(), sourcePath);
    }
  }

  static File convertPathToAbsoluteFile(byte[] path,
      Path localBackupPath, String snapshot) {
    String sourcePath = new String(path);
    if (sourcePath.equals(".")) {
      return new File(localBackupPath.toString());
    } else {
      String snapshotPathPiece = DOT_SNAPSHOT_DIR + Path.SEPARATOR + snapshot;
      Path snapshotPath = new Path(localBackupPath, snapshotPathPiece);
      return new File(snapshotPath.toString(), sourcePath);
    }
  }

  public FileAndDirsSyncTasks createPlanForDirectory(DiffReportEntry diffEntry,
      String targetName, SyncMount syncMount, int snapshotId) {

    try {
      byte[] path = diffEntry.getSourcePath();
      String absolutePath;
      if (diffEntry.getType() == SnapshotDiffReport.DiffType.DELETE) {
        /* Deleted directories only have the inode in the .snapshot/<snapshot>/
         * dir.
         */
        INode snapshottableINode = fsDirectory.getINode(
            syncMount.getLocalPath().toString());
        DirectorySnapshottableFeature dsf = snapshottableINode.asDirectory()
            .getDirectorySnapshottableFeature();
        Snapshot snapshot = dsf.getSnapshotById(snapshotId);
        String snapshotName = Snapshot.getSnapshotName(snapshot);
        absolutePath = convertPathToAbsoluteFile(path,
            syncMount.getLocalPath(), snapshotName).getAbsolutePath();
      } else {
        absolutePath = convertPathToAbsoluteFile(path,
            syncMount.getLocalPath()).getAbsolutePath();
      }

      INodeDirectory nodeDir = fsDirectory.getINode(absolutePath).asDirectory();
      FileAndDirsSyncTasks plan = new FileAndDirsSyncTasks();

      URI remotePath = createRemotePathFromAbsolutePath(syncMount,
          nodeDir.getFullPathName());
      switch (diffEntry.getType()) {
      case CREATE:
        SyncTask.CreateDirectorySyncTask createDir = SyncTask.createDirectory(remotePath,
            syncMount.getName());
        plan.addDirSync(createDir);
        break;
      case DELETE:
        SyncTask.DeleteDirectorySyncTask deleteDir = SyncTask.deleteDirectory(
            remotePath, syncMount.getName());
        plan.addDirSync(deleteDir);
        break;
      default:
        LOG.error("createPlanForDirectory called on directory that had diff {}",
            diffEntry.getInodeType());
      }

      for (INode inode : nodeDir.getChildrenList(snapshotId)) {
        FileAndDirsSyncTasks subPlan =
            createPlanForINode(diffEntry, snapshotId, inode, syncMount);
        plan.append(subPlan);
      }

      return plan;
    } catch (IOException e) {
      throw new RuntimeException("Unhandled error when creating sync service " +
          "plan", e);
    }
  }

  private FileAndDirsSyncTasks createPlanForINode(
      DiffReportEntry diffEntry, int snapshotId, INode node,
      SyncMount syncMount) throws IOException {

    File fullPath = new File(node.getFullPathName());
    if (syncServiceFileFilter.isExcluded(fullPath)) {
      return new FileAndDirsSyncTasks();
    } else if (node.isDirectory()) {
      return recursivelyCreateSyncTaskFromINodeDirectory(diffEntry, snapshotId,
          node.asDirectory(), syncMount);
    } else if (node.isFile()) {
      switch (diffEntry.getType()) {
      case CREATE: {
        SyncTask createdFile =
            filePlanner.createCreatedFileSyncTasks(snapshotId, node.asFile(),
                syncMount);
        FileAndDirsSyncTasks createFileAndDirs = new FileAndDirsSyncTasks();
        createFileAndDirs.addFileSync(createdFile);
        return createFileAndDirs;
      }
      case DELETE: {
        SyncTask deletedFile = filePlanner.createDeletedFileSyncTasks(
            snapshotId, node.asFile(), syncMount);
        FileAndDirsSyncTasks createFileAndDirs = new FileAndDirsSyncTasks();
        createFileAndDirs.addFileSync(deletedFile);
        return createFileAndDirs;
      }
      default:
        LOG.error("createPlanForINode called on directory that had diff {}",
            diffEntry.getInodeType());
        throw new IllegalArgumentException("Unsupported diff type for inode");
      }
    } else {
      LOG.trace("Not backing up created INode {} as it has an unsupported type", node);
      throw new IllegalArgumentException("TODO");
    }
  }

  private FileAndDirsSyncTasks
  recursivelyCreateSyncTaskFromINodeDirectory(DiffReportEntry diffEntry,
      int snapshotId, INodeDirectory nodeDir, SyncMount syncMount)
      throws IOException {

    FileAndDirsSyncTasks plan
        = new FileAndDirsSyncTasks();
    URI remotePath = createRemotePathFromAbsolutePath(syncMount,
        nodeDir.getFullPathName());
    SyncTask.CreateDirectorySyncTask createDir = SyncTask.createDirectory(remotePath,
        syncMount.getName());

    plan.addDirSync(createDir);

    for (INode inode : nodeDir.getChildrenList(snapshotId)) {
      File fullPath = new File(inode.getFullPathName());

      if (!syncServiceFileFilter.isExcluded(fullPath)) {
        FileAndDirsSyncTasks subPlan =
            createPlanForINode(diffEntry, snapshotId, inode, syncMount);
        plan.append(subPlan);
      } else {
        LOG.debug("Not backing up INode {} as excluded by filter", inode);
      }
    }

    return plan;
  }
}
