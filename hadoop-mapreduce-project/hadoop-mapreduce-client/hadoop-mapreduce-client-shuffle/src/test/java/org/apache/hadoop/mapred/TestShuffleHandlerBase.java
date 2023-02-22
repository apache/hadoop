/*
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

package org.apache.hadoop.mapred;

import io.netty.util.ResourceLeakDetector;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheLoader;
import org.apache.hadoop.thirdparty.com.google.common.cache.LoadingCache;
import org.apache.hadoop.thirdparty.com.google.common.cache.RemovalListener;
import org.junit.After;
import org.junit.Before;

import static io.netty.util.ResourceLeakDetector.Level.PARANOID;
import static org.apache.hadoop.io.MapFile.DATA_FILE_NAME;
import static org.apache.hadoop.io.MapFile.INDEX_FILE_NAME;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestShuffleHandlerBase {
  public static final String TEST_ATTEMPT_1 = "attempt_1111111111111_0001_m_000001_0";
  public static final String TEST_ATTEMPT_2 = "attempt_1111111111111_0002_m_000002_0";
  public static final String TEST_ATTEMPT_3 = "attempt_1111111111111_0003_m_000003_0";
  public static final String TEST_JOB_ID = "job_1111111111111_0001";
  public static final String TEST_USER = System.getProperty("user.name");
  public static final String TEST_DATA_A = "aaaaa";
  public static final String TEST_DATA_B = "bbbbb";
  public static final String TEST_DATA_C = "ccccc";

  private final PrintStream standardOut = System.out;
  private final ByteArrayOutputStream outputStreamCaptor = new ByteArrayOutputStream();
  @SuppressWarnings("checkstyle:VisibilityModifier")
  protected java.nio.file.Path tempDir;

  @Before
  public void setup() throws IOException {
    tempDir = Files.createTempDirectory("test-shuffle-channel-handler");
    tempDir.toFile().deleteOnExit();

    generateMapOutput(TEST_USER, tempDir.toAbsolutePath().toString(), TEST_ATTEMPT_1,
        Arrays.asList(TEST_DATA_A, TEST_DATA_B, TEST_DATA_C));
    generateMapOutput(TEST_USER, tempDir.toAbsolutePath().toString(), TEST_ATTEMPT_2,
        Arrays.asList(TEST_DATA_B, TEST_DATA_A, TEST_DATA_C));
    generateMapOutput(TEST_USER, tempDir.toAbsolutePath().toString(), TEST_ATTEMPT_3,
        Arrays.asList(TEST_DATA_C, TEST_DATA_B, TEST_DATA_A));

    outputStreamCaptor.reset();
    ResourceLeakDetector.setLevel(PARANOID);
    System.setOut(new PrintStream(outputStreamCaptor));
  }

  @After
  public void teardown() {
    System.setOut(standardOut);
    System.out.print(outputStreamCaptor);
    // For this to work ch.qos.logback.classic is needed for some reason
    assertFalse(outputStreamCaptor.toString()
        .contains("LEAK: ByteBuf.release() was not called before"));
  }

  public List<String> matchLogs(String pattern) {
    String logs = outputStreamCaptor.toString();
    Matcher m = Pattern.compile(pattern).matcher(logs);
    List<String> allMatches = new ArrayList<>();
    while (m.find()) {
      allMatches.add(m.group());
    }
    return allMatches;
  }

  public static void generateMapOutput(String user, String tempDir,
                                       String attempt, List<String> maps)
      throws IOException {
    SpillRecord record = new SpillRecord(maps.size());

    assertTrue(new File(getBasePath(user, tempDir, attempt)).mkdirs());
    try (PrintWriter writer = new PrintWriter(getDataFile(user, tempDir, attempt), "UTF-8")) {
      long startOffset = 0;
      int partition = 0;
      for (String map : maps) {
        record.putIndex(new IndexRecord(
                startOffset,
                map.length() * 2L, // doesn't matter in this test
                map.length()),
            partition);
        startOffset += map.length() + 1;
        partition++;
        writer.write(map);
      }
      record.writeToFile(new Path(getIndexFile(user, tempDir, attempt)),
          new JobConf(new Configuration()));
    }
  }

  public static String getIndexFile(String user, String tempDir, String attempt) {
    return String.format("%s/%s", getBasePath(user, tempDir, attempt), INDEX_FILE_NAME);
  }

  public static String getDataFile(String user, String tempDir, String attempt) {
    return String.format("%s/%s", getBasePath(user, tempDir, attempt), DATA_FILE_NAME);
  }

  private static String getBasePath(String user, String tempDir, String attempt) {
    return String.format("%s/%s/%s/%s", tempDir, TEST_JOB_ID, user, attempt);
  }

  public static String getUri(String jobId, int reduce, List<String> maps, boolean keepAlive) {
    return String.format("/mapOutput?job=%s&reduce=%d&map=%s%s",
        jobId, reduce, String.join(",", maps),
        keepAlive ? "&keepAlive=true" : "");
  }

  public LoadingCache<ShuffleHandler.AttemptPathIdentifier,
      ShuffleHandler.AttemptPathInfo> createLoadingCache() {
    return CacheBuilder.newBuilder().expireAfterAccess(
            5,
            TimeUnit.MINUTES).softValues().concurrencyLevel(16).
        removalListener(
            (RemovalListener<ShuffleHandler.AttemptPathIdentifier,
                ShuffleHandler.AttemptPathInfo>) notification -> {
            }
        ).maximumWeight(10 * 1024 * 1024).weigher(
            (key, value) -> key.jobId.length() + key.user.length() +
                key.attemptId.length() +
                value.indexPath.toString().length() +
                value.dataPath.toString().length()
        ).build(new CacheLoader<ShuffleHandler.AttemptPathIdentifier,
            ShuffleHandler.AttemptPathInfo>() {
          @Override
          public ShuffleHandler.AttemptPathInfo load(
              @Nonnull ShuffleHandler.AttemptPathIdentifier key) {
            String base = String.format("%s/%s/%s/", tempDir, key.jobId, key.user);
            String attemptBase = base + key.attemptId;
            Path indexFileName = new Path(attemptBase + "/" + INDEX_FILE_NAME);
            Path mapOutputFileName = new Path(attemptBase + "/" + DATA_FILE_NAME);
            return new ShuffleHandler.AttemptPathInfo(indexFileName, mapOutputFileName);
          }
        });
  }
}
