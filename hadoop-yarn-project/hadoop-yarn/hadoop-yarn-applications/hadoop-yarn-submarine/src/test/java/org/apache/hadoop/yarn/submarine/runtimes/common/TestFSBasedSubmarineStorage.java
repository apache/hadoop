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

package org.apache.hadoop.yarn.submarine.runtimes.common;

import org.apache.hadoop.yarn.submarine.common.ClientContext;
import org.apache.hadoop.yarn.submarine.common.fs.MockRemoteDirectoryManager;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestFSBasedSubmarineStorage {
  private Map<String, String> getMap(String prefix) {
    Map<String, String> map = new HashMap<>();
    map.put(prefix + "1", "1");
    map.put(prefix + "2", "2");
    map.put(prefix + "3", "3");
    map.put(prefix + "4", "4");
    return map;
  }

  private void compareMap(Map<String, String> map1, Map<String, String> map2) {
    Assert.assertEquals(map1.size(), map2.size());
    for (String k : map1.keySet()) {
      Assert.assertEquals(map1.get(k), map2.get(k));
    }
  }

  @Test
  public void testStorageOps() throws IOException {
    MockRemoteDirectoryManager remoteDirectoryManager = new MockRemoteDirectoryManager();
    ClientContext clientContext = mock(ClientContext.class);
    when(clientContext.getRemoteDirectoryManager()).thenReturn(remoteDirectoryManager);
    FSBasedSubmarineStorageImpl storage = new FSBasedSubmarineStorageImpl(
        clientContext);
    storage.addNewJob("job1", getMap("job1"));
    storage.addNewJob("job2", getMap("job2"));
    storage.addNewJob("job3", getMap("job3"));
    storage.addNewJob("job4", new HashMap<>());
    storage.addNewModel("model1", "1.0", getMap("model1_1.0"));
    storage.addNewModel("model1", "2.0.0", getMap("model1_2.0.0"));
    storage.addNewModel("model2", null, getMap("model1_default"));
    storage.addNewModel("model2", "1.0", getMap("model2_1.0"));

    // create a new storage and read it back.
    storage = new FSBasedSubmarineStorageImpl(
        clientContext);
    compareMap(getMap("job1"), storage.getJobInfoByName("job1"));
    compareMap(getMap("job2"), storage.getJobInfoByName("job2"));
    compareMap(getMap("job3"), storage.getJobInfoByName("job3"));
    compareMap(new HashMap<>(), storage.getJobInfoByName("job4"));
    compareMap(getMap("model1_1.0"), storage.getModelInfoByName("model1", "1.0"));
    compareMap(getMap("model1_2.0.0"), storage.getModelInfoByName("model1", "2.0.0"));
    compareMap(getMap("model2_1.0"), storage.getModelInfoByName("model2", "1.0"));
  }
}
