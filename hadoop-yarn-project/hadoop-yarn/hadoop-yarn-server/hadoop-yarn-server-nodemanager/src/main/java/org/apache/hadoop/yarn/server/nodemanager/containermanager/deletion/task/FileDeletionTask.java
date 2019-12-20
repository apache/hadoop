/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.nodemanager.containermanager.deletion.task;

import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.DeletionServiceDeleteTaskProto;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.executor.DeletionAsUserContext;

import java.io.IOException;
import java.util.List;

/**
 * {@link DeletionTask} handling the removal of files (and directories).
 */
public class FileDeletionTask extends DeletionTask implements Runnable {

  private final Path subDir;
  private final List<Path> baseDirs;
  private static final FileContext lfs = getLfs();

  private static FileContext getLfs() {
    try {
      return FileContext.getLocalFSFileContext();
    } catch (UnsupportedFileSystemException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Construct a FileDeletionTask with the default INVALID_TASK_ID.
   *
   * @param deletionService     the {@link DeletionService}.
   * @param user                the user deleting the file.
   * @param subDir              the subdirectory to delete.
   * @param baseDirs            the base directories containing the subdir.
   */
  public FileDeletionTask(DeletionService deletionService, String user,
      Path subDir, List<Path> baseDirs) {
    this(INVALID_TASK_ID, deletionService, user, subDir, baseDirs);
  }

  /**
   * Construct a FileDeletionTask with the default INVALID_TASK_ID.
   *
   * @param taskId              the ID of the task, if previously set.
   * @param deletionService     the {@link DeletionService}.
   * @param user                the user deleting the file.
   * @param subDir              the subdirectory to delete.
   * @param baseDirs            the base directories containing the subdir.
   */
  public FileDeletionTask(int taskId, DeletionService deletionService,
      String user, Path subDir, List<Path> baseDirs) {
    super(taskId, deletionService, user, DeletionTaskType.FILE);
    this.subDir = subDir;
    this.baseDirs = baseDirs;
  }

  /**
   * Get the subdirectory to delete.
   *
   * @return the subDir for the FileDeletionTask.
   */
  public Path getSubDir() {
    return this.subDir;
  }

  /**
   * Get the base directories containing the subdirectory.
   *
   * @return the base directories for the FileDeletionTask.
   */
  public List<Path> getBaseDirs() {
    return this.baseDirs;
  }

  /**
   * Delete the specified file/directory as the specified user.
   */
  @Override
  public void run() {
    LOG.debug("Running DeletionTask : {}", this);
    boolean error = false;
    if (null == getUser()) {
      if (baseDirs == null || baseDirs.size() == 0) {
        LOG.debug("NM deleting absolute path : {}", subDir);
        try {
          lfs.delete(subDir, true);
        } catch (IOException e) {
          error = true;
          LOG.warn("Failed to delete " + subDir);
        }
      } else {
        for (Path baseDir : baseDirs) {
          Path del = subDir == null? baseDir : new Path(baseDir, subDir);
          LOG.debug("NM deleting path : {}", del);
          try {
            lfs.delete(del, true);
          } catch (IOException e) {
            error = true;
            LOG.warn("Failed to delete " + subDir);
          }
        }
      }
    } else {
      try {
        LOG.debug("Deleting path: [{}] as user [{}]", subDir, getUser());
        if (baseDirs == null || baseDirs.size() == 0) {
          getDeletionService().getContainerExecutor().deleteAsUser(
              new DeletionAsUserContext.Builder()
              .setUser(getUser())
              .setSubDir(subDir)
              .build());
        } else {
          getDeletionService().getContainerExecutor().deleteAsUser(
              new DeletionAsUserContext.Builder()
              .setUser(getUser())
              .setSubDir(subDir)
              .setBasedirs(baseDirs.toArray(new Path[0]))
              .build());
        }
      } catch (IOException|InterruptedException e) {
        error = true;
        LOG.warn("Failed to delete as user " + getUser(), e);
      }
    }
    if (error) {
      setSuccess(!error);
    }
    deletionTaskFinished();
  }

  /**
   * Convert the FileDeletionTask to a String representation.
   *
   * @return String representation of the FileDeletionTask.
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("FileDeletionTask :");
    sb.append("  id : ").append(getTaskId());
    sb.append("  user : ").append(getUser());
    sb.append("  subDir : ").append(
        subDir == null ? "null" : subDir.toString());
    sb.append("  baseDir : ");
    if (baseDirs == null || baseDirs.size() == 0) {
      sb.append("null");
    } else {
      for (Path baseDir : baseDirs) {
        sb.append(baseDir.toString()).append(',');
      }
    }
    return sb.toString().trim();
  }

  /**
   * Convert the FileDeletionTask to the Protobuf representation for storing
   * in the state store and recovery.
   *
   * @return the protobuf representation of the FileDeletionTask.
   */
  public DeletionServiceDeleteTaskProto convertDeletionTaskToProto() {
    DeletionServiceDeleteTaskProto.Builder builder =
        getBaseDeletionTaskProtoBuilder();
    builder.setTaskType(DeletionTaskType.FILE.name());
    if (getSubDir() != null) {
      builder.setSubdir(getSubDir().toString());
    }
    if (getBaseDirs() != null) {
      for (Path dir : getBaseDirs()) {
        builder.addBasedirs(dir.toString());
      }
    }
    return builder.build();
  }
}
