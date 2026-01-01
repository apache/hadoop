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
package org.apache.hadoop.hdfs.net;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NET_TOPOLOGY_WEIGHT_TABLE_FILE_NAME_KEY;
import static org.apache.hadoop.hdfs.net.AbstractDataNodeWeightMapping.DEFAULT_WEIGHT;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.thirdparty.com.google.common.io.Files;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 60)
public class TestTableDataNodeWeightMapping {

  private final String ipAddress1 = "1.2.3.4";
  private final String ipAddress2 = "5.6.7.8";
  private final String ipAddress3 = "9.10.11.12";

  @Test
  public void testResolve() throws IOException {
    File mapFile = createTempTableFile("testResolve");
    writeTableFile(mapFile,
        new String[]{ipAddress1, ipAddress2},
        new int[]{6, 3});

    TableDataNodeWeightMapping mapping = new TableDataNodeWeightMapping();
    setTableFilePathConfig(mapping, mapFile.getCanonicalPath());

    assertEquals(6, mapping.resolve(ipAddress1));
    assertEquals(3, mapping.resolve(ipAddress2));
  }

  @Test
  public void testNoFile() {
    TableDataNodeWeightMapping mapping = new TableDataNodeWeightMapping();

    Configuration conf = new Configuration();
    mapping.setConf(conf);

    assertEquals(DEFAULT_WEIGHT, mapping.resolve(ipAddress1));
    assertEquals(DEFAULT_WEIGHT, mapping.resolve(ipAddress2));
  }

  @Test
  public void testFileDoesNotExist() {
    TableDataNodeWeightMapping mapping = new TableDataNodeWeightMapping();
    setTableFilePathConfig(mapping, "/this/file/does/not/exist");

    assertEquals(DEFAULT_WEIGHT, mapping.resolve(ipAddress1));
    assertEquals(DEFAULT_WEIGHT, mapping.resolve(ipAddress2));
  }

  @Test
  public void testReload() throws IOException {
    File mapFile = createTempTableFile("testResolve");

    writeTableFile(mapFile,
        new String[]{ipAddress1, ipAddress2},
        new int[]{6, 3});
    TableDataNodeWeightMapping mapping = new TableDataNodeWeightMapping();
    setTableFilePathConfig(mapping, mapFile.getCanonicalPath());

    assertEquals(6, mapping.resolve(ipAddress1));
    assertEquals(3, mapping.resolve(ipAddress2));
    assertEquals(DEFAULT_WEIGHT, mapping.resolve(ipAddress3));

    writeTableFile(mapFile,
        new String[]{ipAddress1, ipAddress3},
        new int[]{5, 2});

    mapping.reload();
    assertEquals(5, mapping.resolve(ipAddress1));
    assertEquals(DEFAULT_WEIGHT, mapping.resolve(ipAddress2));
    assertEquals(2, mapping.resolve(ipAddress3));

    writeTableFile(mapFile, new String[]{}, new int[]{});

    mapping.reload();
    assertEquals(DEFAULT_WEIGHT, mapping.resolve(ipAddress1));
    assertEquals(DEFAULT_WEIGHT, mapping.resolve(ipAddress2));
    assertEquals(DEFAULT_WEIGHT, mapping.resolve(ipAddress3));
  }

  @Test
  public void testBadFile() throws IOException {
    File mapFile = createTempTableFile("testBadFile");
    Files.asCharSink(mapFile, StandardCharsets.UTF_8).write("bad contents");

    TableDataNodeWeightMapping mapping = new TableDataNodeWeightMapping();
    setTableFilePathConfig(mapping, mapFile.getCanonicalPath());

    assertEquals(DEFAULT_WEIGHT, mapping.resolve(ipAddress1));
    assertEquals(DEFAULT_WEIGHT, mapping.resolve(ipAddress2));
  }

  private void setTableFilePathConfig(DNSToWeightMapping mapping, String mapFilePath) {
    Configuration conf = new Configuration();
    conf.set(DFS_NET_TOPOLOGY_WEIGHT_TABLE_FILE_NAME_KEY, mapFilePath);
    mapping.setConf(conf);
  }

  private File createTempTableFile(String name) throws IOException {
    File mapFile = File.createTempFile(getClass().getSimpleName() + "." + name,
        ".txt");
    mapFile.deleteOnExit();
    return mapFile;
  }

  private void writeTableFile(File mapFile, String[] ipAddresses, int[] weights)
      throws IOException {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < ipAddresses.length; i++) {
      sb.append(ipAddresses[i]);
      sb.append("\t");
      sb.append(weights[i]);
      sb.append("\n");
    }
    Files.asCharSink(mapFile, StandardCharsets.UTF_8).write(sb.toString());
  }

}
