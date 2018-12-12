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

package org.apache.hadoop.yarn.submarine.common.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;

public class MockRemoteDirectoryManager implements RemoteDirectoryManager {
  private File jobsParentDir = null;
  private File modelParentDir = null;

  private File jobDir = null;
  @Override
  public Path getJobStagingArea(String jobName, boolean create)
      throws IOException {
    if (jobsParentDir == null && create) {
      jobsParentDir = new File(
          "target/_staging_area_" + System.currentTimeMillis());
      if (!jobsParentDir.mkdirs()) {
        throw new IOException(
            "Failed to mkdirs for" + jobsParentDir.getAbsolutePath());
      }
    }

    this.jobDir = new File(jobsParentDir.getAbsolutePath(), jobName);
    if (create && !jobDir.exists()) {
      if (!jobDir.mkdirs()) {
        throw new IOException("Failed to mkdirs for "
            + jobDir.getAbsolutePath());
      }
    }
    return new Path(jobDir.getAbsolutePath());
  }

  @Override
  public Path getJobCheckpointDir(String jobName, boolean create)
      throws IOException {
    return new Path("s3://generated_checkpoint_dir");
  }

  @Override
  public Path getModelDir(String modelName, boolean create)
      throws IOException {
    if (modelParentDir == null && create) {
      modelParentDir = new File(
          "target/_models_" + System.currentTimeMillis());
      if (!modelParentDir.mkdirs()) {
        throw new IOException(
            "Failed to mkdirs for " + modelParentDir.getAbsolutePath());
      }
    }

    File modelDir = new File(modelParentDir.getAbsolutePath(), modelName);
    if (create) {
      if (!modelDir.exists() && !modelDir.mkdirs()) {
        throw new IOException("Failed to mkdirs for "
            + modelDir.getAbsolutePath());
      }
    }
    return new Path(modelDir.getAbsolutePath());
  }

  @Override
  public FileSystem getDefaultFileSystem() throws IOException {
    return FileSystem.getLocal(new Configuration());
  }

  @Override
  public FileSystem getFileSystemByUri(String uri) throws IOException {
    return getDefaultFileSystem();
  }

  @Override
  public Path getUserRootFolder() throws IOException {
    return new Path("s3://generated_root_dir");
  }

  @Override
  public boolean isDir(String uri) throws IOException {
    return getDefaultFileSystem().getFileStatus(
        new Path(convertToStagingPath(uri))).isDirectory();

  }

  @Override
  public boolean isRemote(String uri) throws IOException {
    String scheme = new Path(uri).toUri().getScheme();
    if (null == scheme) {
      return false;
    }
    return !scheme.startsWith("file://");
  }

  private String convertToStagingPath(String uri) throws IOException {
    String ret = uri;
    if (isRemote(uri)) {
      String dirName = new Path(uri).getName();
      ret = this.jobDir.getAbsolutePath()
          + "/" + dirName;
    }
    return ret;
  }

  /**
   * We use staging dir as mock HDFS dir.
   * */
  @Override
  public boolean copyRemoteToLocal(String remoteUri, String localUri)
      throws IOException {
    // mock the copy from HDFS into a local copy
    Path remoteToLocalDir = new Path(convertToStagingPath(remoteUri));
    File old = new File(convertToStagingPath(localUri));
    if (old.isDirectory() && old.exists()) {
      if (!FileUtil.fullyDelete(old)) {
        throw new IOException("Cannot delete temp dir:"
            + old.getAbsolutePath());
      }
    }
    return FileUtil.copy(getDefaultFileSystem(), remoteToLocalDir,
        new File(localUri), false,
        getDefaultFileSystem().getConf());
  }

  @Override
  public boolean existsRemoteFile(Path uri) throws IOException {
    String fakeLocalFilePath = this.jobDir.getAbsolutePath()
        + "/" + uri.getName();
    return new File(fakeLocalFilePath).exists();
  }

  @Override
  public FileStatus getRemoteFileStatus(Path p) throws IOException {
    return getDefaultFileSystem().getFileStatus(new Path(
        convertToStagingPath(p.toUri().toString())));
  }

  @Override
  public long getRemoteFileSize(String uri) throws IOException {
    // 5 byte for this file to test
    if (uri.equals("https://a/b/1.patch")) {
      return 5;
    }
    return 100 * 1024 * 1024;
  }

}
