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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.test.GenericTestUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestNNUpdateStorageVersionWhenInterrupt {
  private static final String nnDir =
      GenericTestUtils.getTestDir("dfs").getAbsolutePath() + File.separator + "namenode";
  private static NNStorage nnStorage;
  private static GenericTestUtils.LogCapturer nnStorageLog;

  @BeforeAll
  public static void setUp() throws IOException, URISyntaxException {
    String scheme = "file:///";
    Collection<URI> dirs = new ArrayList<>();
    dirs.add(new URI(scheme + nnDir));
    nnStorage = new NNStorage(new Configuration(), dirs, dirs);

    StorageDirectory sd = new StorageDirectory(new File(nnDir));
    Path versionFile = sd.getVersionFile().toPath();
    Files.createDirectories(versionFile.getParent());
    if (!Files.exists(versionFile)) {
      Files.createFile(versionFile);
    }

    nnStorageLog = GenericTestUtils.LogCapturer.captureLogs(NNStorage.LOG);
  }

  @Test
  public void test()
      throws IOException, URISyntaxException, InterruptedException, TimeoutException {
    Thread thread = new UpdateVersionFileThread(nnStorage);
    assertEquals(1, nnStorage.getNumStorageDirs());

    thread.start();
    thread.interrupt();

    GenericTestUtils.waitFor(
        () -> nnStorageLog.getOutput().contains("java.nio.channels.ClosedByInterruptException"),
        200, 20000);
    assertEquals(1, nnStorage.getNumStorageDirs());
  }

  private static class UpdateVersionFileThread extends Thread {
    NNStorage nnStorage;

    UpdateVersionFileThread(NNStorage nnStorage) {
      this.nnStorage = nnStorage;
    }

    @Override
    public void run() {
      try {
        nnStorage.writeAll();
      } catch (IOException ignored) {

      }
    }
  }

}
