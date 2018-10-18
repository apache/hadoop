/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package org.apache.hadoop.yarn.submarine.common.fs;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.submarine.client.cli.CliConstants;
import org.apache.hadoop.yarn.submarine.common.ClientContext;

import java.io.IOException;

/**
 * Manages remote directories for staging, log, etc.
 * TODO, need to properly handle permission / name validation, etc.
 */
public class DefaultRemoteDirectoryManager implements RemoteDirectoryManager {
  FileSystem fs;

  public DefaultRemoteDirectoryManager(ClientContext context) {
    try {
      this.fs = FileSystem.get(context.getYarnConfig());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Path getJobStagingArea(String jobName, boolean create) throws IOException {
    Path staging = new Path(getJobRootFolder(jobName), "staging");
    if (create) {
      createFolderIfNotExist(staging);
    }

    // Get a file status to make sure it is a absolute path.
    FileStatus fStatus = fs.getFileStatus(staging);
    return fStatus.getPath();
  }

  @Override
  public Path getJobCheckpointDir(String jobName, boolean create)
      throws IOException {
    Path jobDir = new Path(getJobStagingArea(jobName, create),
        CliConstants.CHECKPOINT_PATH);
    if (create) {
      createFolderIfNotExist(jobDir);
    }
    return jobDir;
  }

  @Override
  public Path getModelDir(String modelName, boolean create) throws IOException {
    Path modelDir = new Path(new Path("submarine", "models"), modelName);
    if (create) {
      createFolderIfNotExist(modelDir);
    }
    return modelDir;
  }

  @Override
  public FileSystem getFileSystem() {
    return fs;
  }

  @Override
  public Path getUserRootFolder() throws IOException {
    Path rootPath = new Path("submarine", "jobs");
    createFolderIfNotExist(rootPath);
    // Get a file status to make sure it is a absolute path.
    FileStatus fStatus = fs.getFileStatus(rootPath);
    return fStatus.getPath();
  }

  private Path getJobRootFolder(String jobName) throws IOException {
    Path jobRootPath = getUserRootFolder();
    createFolderIfNotExist(jobRootPath);
    // Get a file status to make sure it is a absolute path.
    FileStatus fStatus = fs.getFileStatus(jobRootPath);
    return fStatus.getPath();
  }

  private void createFolderIfNotExist(Path path) throws IOException {
    if (!fs.exists(path)) {
      if (!fs.mkdirs(path)) {
        throw new IOException("Failed to create folder=" + path);
      }
    }
  }
}
