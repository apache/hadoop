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

package org.apache.hadoop.yarn.submarine.runtimes.yarnservice;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.tensorflow.TensorFlowConfigEnvGenerator;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Class to test some functionality of {@link TensorFlowConfigEnvGenerator}.
 */
public class TestTensorFlowConfigEnvGenerator {
  private ObjectMapper objectMapper;

  @Before
  public void setUp() {
    objectMapper = new ObjectMapper();
  }

  private void verifyCommonJsonData(JsonNode node, String taskType) {
    JsonNode task = node.get("task");
    assertNotNull(task);
    assertEquals(taskType, task.get("type").asText());
    assertEquals("$_TASK_INDEX", task.get("index").asText());

    JsonNode environment = task.get("environment");
    assertNotNull(environment);
    assertEquals("cloud", environment.asText());
  }

  private void verifyArrayElements(JsonNode node, String childName,
      String... elements) {
    JsonNode master = node.get(childName);
    assertNotNull(master);
    assertEquals(JsonNodeType.ARRAY, master.getNodeType());
    ArrayNode masterArray = (ArrayNode) master;
    verifyArray(masterArray, elements);
  }

  private void verifyArray(ArrayNode array, String... elements) {
    int arraySize = array.size();
    assertEquals(elements.length, arraySize);

    for (int i = 0; i < arraySize; i++) {
      JsonNode arrayElement = array.get(i);
      assertEquals(elements[i], arrayElement.asText());
    }
  }

  @Test
  public void testSimpleDistributedTFConfigGeneratorWorker()
      throws IOException {
    String json = TensorFlowConfigEnvGenerator.getTFConfigEnv("worker", 5, 3,
        "wtan", "tf-job-001", "example.com");

    JsonNode jsonNode = objectMapper.readTree(json);
    assertNotNull(jsonNode);
    JsonNode cluster = jsonNode.get("cluster");
    assertNotNull(cluster);

    verifyArrayElements(cluster, "master",
        "master-0.wtan.tf-job-001.example.com:8000");
    verifyArrayElements(cluster, "worker",
        "worker-0.wtan.tf-job-001.example.com:8000",
        "worker-1.wtan.tf-job-001.example.com:8000",
        "worker-2.wtan.tf-job-001.example.com:8000",
        "worker-3.wtan.tf-job-001.example.com:8000");

    verifyArrayElements(cluster, "ps", "ps-0.wtan.tf-job-001.example.com:8000",
        "ps-1.wtan.tf-job-001.example.com:8000",
        "ps-2.wtan.tf-job-001.example.com:8000");

    verifyCommonJsonData(jsonNode, "worker");
  }

  @Test
  public void testSimpleDistributedTFConfigGeneratorMaster()
      throws IOException {
    String json = TensorFlowConfigEnvGenerator.getTFConfigEnv("master", 2, 1,
        "wtan", "tf-job-001", "example.com");

    JsonNode jsonNode = objectMapper.readTree(json);
    assertNotNull(jsonNode);
    JsonNode cluster = jsonNode.get("cluster");
    assertNotNull(cluster);

    verifyArrayElements(cluster, "master",
        "master-0.wtan.tf-job-001.example.com:8000");
    verifyArrayElements(cluster, "worker",
        "worker-0.wtan.tf-job-001.example.com:8000");

    verifyArrayElements(cluster, "ps", "ps-0.wtan.tf-job-001.example.com:8000");

    verifyCommonJsonData(jsonNode, "master");
  }

  @Test
  public void testSimpleDistributedTFConfigGeneratorPS() throws IOException {
    String json = TensorFlowConfigEnvGenerator.getTFConfigEnv("ps", 5, 3,
        "wtan", "tf-job-001", "example.com");

    JsonNode jsonNode = objectMapper.readTree(json);
    assertNotNull(jsonNode);
    JsonNode cluster = jsonNode.get("cluster");
    assertNotNull(cluster);

    verifyArrayElements(cluster, "master",
        "master-0.wtan.tf-job-001.example.com:8000");
    verifyArrayElements(cluster, "worker",
        "worker-0.wtan.tf-job-001.example.com:8000",
        "worker-1.wtan.tf-job-001.example.com:8000",
        "worker-2.wtan.tf-job-001.example.com:8000",
        "worker-3.wtan.tf-job-001.example.com:8000");

    verifyArrayElements(cluster, "ps", "ps-0.wtan.tf-job-001.example.com:8000",
        "ps-1.wtan.tf-job-001.example.com:8000",
        "ps-2.wtan.tf-job-001.example.com:8000");

    verifyCommonJsonData(jsonNode, "ps");
  }
}
