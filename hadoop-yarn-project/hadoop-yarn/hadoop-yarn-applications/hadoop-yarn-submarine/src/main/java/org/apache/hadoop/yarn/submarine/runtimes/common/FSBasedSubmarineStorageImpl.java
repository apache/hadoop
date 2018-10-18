/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */


package org.apache.hadoop.yarn.submarine.runtimes.common;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.submarine.common.ClientContext;
import org.apache.hadoop.yarn.submarine.common.fs.RemoteDirectoryManager;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Map;

/**
 * A super naive FS-based storage.
 */
public class FSBasedSubmarineStorageImpl extends SubmarineStorage {
  RemoteDirectoryManager rdm;

  public FSBasedSubmarineStorageImpl(ClientContext clientContext) {
    rdm = clientContext.getRemoteDirectoryManager();
  }

  @Override
  public void addNewJob(String jobName, Map<String, String> jobInfo)
      throws IOException {
    Path jobInfoPath = getJobInfoPath(jobName, true);
    FSDataOutputStream fos = rdm.getFileSystem().create(jobInfoPath);
    serializeMap(fos, jobInfo);
  }

  @Override
  public Map<String, String> getJobInfoByName(String jobName)
      throws IOException {
    Path jobInfoPath = getJobInfoPath(jobName, false);
    FSDataInputStream fis = rdm.getFileSystem().open(jobInfoPath);
    return deserializeMap(fis);
  }

  @Override
  public void addNewModel(String modelName, String version,
      Map<String, String> modelInfo) throws IOException {
    Path modelInfoPath = getModelInfoPath(modelName, version, true);
    FSDataOutputStream fos = rdm.getFileSystem().create(modelInfoPath);
    serializeMap(fos, modelInfo);
  }

  @Override
  public Map<String, String> getModelInfoByName(String modelName,
      String version) throws IOException {
    Path modelInfoPath = getModelInfoPath(modelName, version, false);
    FSDataInputStream fis = rdm.getFileSystem().open(modelInfoPath);
    return deserializeMap(fis);
  }

  private Path getModelInfoPath(String modelName, String version, boolean create)
      throws IOException {
    Path modelDir = rdm.getModelDir(modelName, create);
    Path modelInfo = new Path(modelDir, version + ".info");
    return modelInfo;
  }

  private void serializeMap(FSDataOutputStream fos, Map<String, String> map)
      throws IOException {
    ObjectOutput oo = new ObjectOutputStream(fos);
    oo.writeObject(map);
    oo.close();
  }

  private Map<String, String> deserializeMap(FSDataInputStream fis)
      throws IOException {
    ObjectInput oi = new ObjectInputStream(fis);
    Map<String, String> newMap;
    try {
      newMap = (Map<String, String>) oi.readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
    return newMap;
  }

  private Path getJobInfoPath(String jobName, boolean create) throws IOException {
    Path path = rdm.getJobStagingArea(jobName, create);
    Path fileName = new Path(path, "job.info");
    return fileName;
  }
}
