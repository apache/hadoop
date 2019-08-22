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
package org.apache.hadoop.tools.dynamometer.blockgenerator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;


/** Tests for block generation via {@link GenerateBlockImagesDriver}. */
public class TestBlockGen {
  private static final Logger LOG = LoggerFactory.getLogger(TestBlockGen.class);

  private MiniDFSCluster dfsCluster;
  private FileSystem fs;
  private static final String FS_IMAGE_NAME = "fsimage_0000000000000061740.xml";
  private static final String BLOCK_LIST_OUTPUT_DIR_NAME = "blockLists";
  private Path tmpPath;

  @Before
  public void setup() throws Exception {
    Configuration conf = new Configuration();
    dfsCluster = new MiniDFSCluster.Builder(conf).build();
    dfsCluster.waitActive();
    LOG.info("Started MiniDFSCluster");
    fs = dfsCluster.getFileSystem();
    FileSystem.setDefaultUri(conf, fs.getUri());
    tmpPath = fs.makeQualified(new Path("/tmp"));
    fs.mkdirs(tmpPath);
    String fsImageFile = this.getClass().getClassLoader()
        .getResource(FS_IMAGE_NAME).getPath();

    fs.copyFromLocalFile(new Path(fsImageFile),
        new Path(tmpPath, FS_IMAGE_NAME));
  }

  @After
  public void cleanUp() {
    dfsCluster.shutdown();
  }

  @Test
  public void testBlockGen() throws Exception {
    LOG.info("Started test");

    int datanodeCount = 40;

    GenerateBlockImagesDriver driver = new GenerateBlockImagesDriver(
        new Configuration());
    driver.run(
        new String[] {"-" + GenerateBlockImagesDriver.FSIMAGE_INPUT_PATH_ARG,
            new Path(tmpPath, FS_IMAGE_NAME).toString(),
            "-" + GenerateBlockImagesDriver.BLOCK_IMAGE_OUTPUT_ARG,
            new Path(tmpPath, BLOCK_LIST_OUTPUT_DIR_NAME).toString(),
            "-" + GenerateBlockImagesDriver.NUM_DATANODES_ARG,
            String.valueOf(datanodeCount)});

    for (int i = 0; i < datanodeCount; i++) {
      final int idx = i;
      assertEquals(1, fs.listStatus(
          new Path(tmpPath, BLOCK_LIST_OUTPUT_DIR_NAME),
          (path) -> path.getName().startsWith(String.format("dn%d-", idx))
      ).length);
    }
  }
}
