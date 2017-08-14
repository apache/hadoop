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

package org.apache.slider.server.appmaster.model.history;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.service.conf.SliderKeys;
import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest;
import org.apache.slider.server.appmaster.model.mock.MockFactory;
import org.apache.slider.server.appmaster.model.mock.MockRoleHistory;
import org.apache.slider.server.appmaster.state.NodeEntry;
import org.apache.slider.server.appmaster.state.NodeInstance;
import org.apache.slider.server.appmaster.state.RoleHistory;
import org.apache.slider.server.avro.NewerFilesFirst;
import org.apache.slider.server.avro.RoleHistoryWriter;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Test role history rw ordering.
 */
public class TestRoleHistoryRWOrdering extends BaseMockAppStateTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestRoleHistoryRWOrdering.class);

  private List<Path> paths = pathlist(
      Arrays.asList(
        "hdfs://localhost/history-0406c.json",
        "hdfs://localhost/history-5fffa.json",
        "hdfs://localhost/history-0001a.json",
        "hdfs://localhost/history-0001f.json"
      )
  );
  private Path h0406c = paths.get(0);
  private Path h5fffa = paths.get(1);
  private Path h0001a = paths.get(3);

  public TestRoleHistoryRWOrdering() throws URISyntaxException {
  }

  List<Path> pathlist(List<String> pathnames) throws URISyntaxException {
    List<Path> pathList = new ArrayList<>();
    for (String p : pathnames) {
      pathList.add(new Path(new URI(p)));
    }
    return pathList;
  }

  @Override
  public String getTestName() {
    return "TestHistoryRWOrdering";
  }

  /**
   * This tests regexp pattern matching. It uses the current time so isn't
   * repeatable -but it does test a wider range of values in the process
   * @throws Throwable
   */
  //@Test
  public void testPatternRoundTrip() throws Throwable {
    describe("test pattern matching of names");
    long value=System.currentTimeMillis();
    String name = String.format(SliderKeys.HISTORY_FILENAME_CREATION_PATTERN,
        value);
    String matchpattern = SliderKeys.HISTORY_FILENAME_MATCH_PATTERN;
    Pattern pattern = Pattern.compile(matchpattern);
    Matcher matcher = pattern.matcher(name);
    if (!matcher.find()) {
      throw new Exception("No match for pattern $matchpattern in $name");
    }
  }

  //@Test
  public void testWriteSequenceReadData() throws Throwable {
    describe("test that if multiple entries are written, the newest is picked" +
        " up");
    long time = System.currentTimeMillis();

    RoleHistory roleHistory = new MockRoleHistory(MockFactory.ROLES);
    assertFalse(roleHistory.onStart(fs, historyPath));
    String addr = "localhost";
    NodeInstance instance = roleHistory.getOrCreateNodeInstance(addr);
    NodeEntry ne1 = instance.getOrCreate(0);
    ne1.setLastUsed(0xf00d);

    Path history1 = roleHistory.saveHistory(time++);
    Path history2 = roleHistory.saveHistory(time++);
    Path history3 = roleHistory.saveHistory(time);

    //inject a later file with a different name
    sliderFileSystem.cat(new Path(historyPath, "file.json"), true, "hello," +
        " world");


    RoleHistoryWriter historyWriter = new RoleHistoryWriter();

    List<Path> entries = historyWriter.findAllHistoryEntries(
        fs,
        historyPath,
        false);
    assertEquals(entries.size(), 3);
    assertEquals(entries.get(0), history3);
    assertEquals(entries.get(1), history2);
    assertEquals(entries.get(2), history1);
  }

  //@Test
  public void testPathStructure() throws Throwable {
    assertEquals(h5fffa.getName(), "history-5fffa.json");
  }

  //@Test
  public void testPathnameComparator() throws Throwable {

    NewerFilesFirst newerName = new NewerFilesFirst();

    LOG.info("{} name is {}", h5fffa, h5fffa.getName());
    LOG.info("{} name is {}", h0406c, h0406c.getName());
    assertEquals(newerName.compare(h5fffa, h5fffa), 0);
    assertTrue(newerName.compare(h5fffa, h0406c) < 0);
    assertTrue(newerName.compare(h5fffa, h0001a) < 0);
    assertTrue(newerName.compare(h0001a, h5fffa) > 0);

  }

  //@Test
  public void testPathSort() throws Throwable {
    List<Path> paths2 = new ArrayList<>(paths);
    RoleHistoryWriter.sortHistoryPaths(paths2);
    assertListEquals(paths2,
                     Arrays.asList(
                       paths.get(1),
                       paths.get(0),
                       paths.get(3),
                       paths.get(2)
                     ));
  }
}
