package org.apache.hadoop.tools.rumen;

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

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.ToolRunner;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestRumenFolder {
  @Test
  public void testFoldingSmallTrace() throws Exception {
    final Configuration conf = new Configuration();
    final FileSystem lfs = FileSystem.getLocal(conf);

    @SuppressWarnings("deprecation")
    final Path rootInputDir =
        new Path(System.getProperty("test.tools.input.dir", ""))
            .makeQualified(lfs);
    @SuppressWarnings("deprecation")
    final Path rootTempDir =
        new Path(System.getProperty("test.build.data", "/tmp"))
            .makeQualified(lfs);

    final Path rootInputFile = new Path(rootInputDir, "rumen/small-trace-test");
    final Path tempDir = new Path(rootTempDir, "TestRumenJobTraces");
    lfs.delete(tempDir, true);

    final Path foldedTracePath = new Path(tempDir, "folded-trace.json");

    final Path inputFile =
        new Path(rootInputFile, "folder-input-trace.json.gz");

    System.out.println("folded trace result path = " + foldedTracePath);

    String[] args =
        { "-input-cycle", "100S", "-output-duration", "300S",
            "-skew-buffer-length", "1", "-seed", "100", "-concentration", "2",
            inputFile.toString(), foldedTracePath.toString() };

    final Path foldedGoldFile =
        new Path(rootInputFile, "goldFoldedTrace.json.gz");

    Folder folder = new Folder();
    int result = ToolRunner.run(folder, args);
    assertEquals("Non-zero exit", 0, result);

    TestRumenFolder.<LoggedJob> jsonFileMatchesGold(conf, lfs, foldedTracePath,
        foldedGoldFile, LoggedJob.class, "trace");
  }

  static private <T extends DeepCompare> void jsonFileMatchesGold(
      Configuration conf, FileSystem lfs, Path result, Path gold,
      Class<? extends T> clazz, String fileDescription) throws IOException {
    JsonObjectMapperParser<T> goldParser =
        new JsonObjectMapperParser<T>(gold, clazz, conf);
    InputStream resultStream = lfs.open(result);
    JsonObjectMapperParser<T> resultParser =
        new JsonObjectMapperParser<T>(resultStream, clazz);
    try {
      while (true) {
        DeepCompare goldJob = goldParser.getNext();
        DeepCompare resultJob = resultParser.getNext();
        if ((goldJob == null) || (resultJob == null)) {
          assertTrue(goldJob == resultJob);
          break;
        }

        try {
          resultJob.deepCompare(goldJob, new TreePath(null, "<root>"));
        } catch (DeepInequalityException e) {
          String error = e.path.toString();

          assertFalse(fileDescription + " mismatches: " + error, true);
        }
      }
    } finally {
      IOUtils.cleanup(null, goldParser, resultParser);
    }
  }
}
