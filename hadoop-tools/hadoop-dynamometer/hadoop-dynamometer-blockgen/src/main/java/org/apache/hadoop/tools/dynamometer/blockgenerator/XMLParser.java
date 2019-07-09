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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// Checkstyle complains about the XML tags even though they are wrapped
// within <pre> and {@code} tags. The SupressWarnings has to go before the
// Javadoc to take effect.
@SuppressWarnings("checkstyle:javadocstyle")
/**
 * This class parses an fsimage file in XML format. It accepts the file
 * line-by-line and maintains an internal state machine to keep track of
 * contextual information. A single parser must process the entire file with the
 * lines in the order they appear in the original file.
 *
 * A file may be spread across multiple lines, so we need to track the
 * replication of the file we are currently processing to be aware of what the
 * replication factor is for each block we encounter. This is why we require a
 * single mapper.
 *
 * The format is illustrated below (line breaks for readability):
 * <pre>{@code
 * <inode><id>inode_ID<id/> <type>inode_type</type>
 * <replication>inode_replication</replication> [file attributes] <blocks>
 * <block><id>XXX</id><genstamp>XXX</genstamp><numBytes>XXX</numBytes><block/>
 * <blocks/> <inode/>
 * }</pre>
 *
 * This is true in both Hadoop 2 and 3.
 */
class XMLParser {

  private static final Pattern BLOCK_PATTERN = Pattern.compile("<block>"
      + "<id>(\\d+)</id>"
      + "<genstamp>(\\d+)</genstamp>"
      + "<numBytes>(\\d+)</numBytes>"
      + "</block>");

  private State currentState = State.DEFAULT;
  private short currentReplication;

  /**
   * Accept a single line of the XML file, and return a {@link BlockInfo} for
   * any blocks contained within that line. Update internal state dependent on
   * other XML values seen, e.g. the beginning of a file.
   *
   * @param line The XML line to parse.
   * @return {@code BlockInfo}s for any blocks found.
   */
  List<BlockInfo> parseLine(String line) throws IOException {
    if (currentState == State.DEFAULT) {
      if (line.contains("<INodeSection>")) {
        transitionTo(State.INODE_SECTION);
      } else {
        return Collections.emptyList();
      }
    }
    if (line.contains("<inode>")) {
      transitionTo(State.INODE);
    }
    if (line.contains("<type>FILE</type>")) {
      transitionTo(State.FILE);
    }
    List<String> replicationStrings = valuesFromXMLString(line, "replication");
    if (!replicationStrings.isEmpty()) {
      if (replicationStrings.size() > 1) {
        throw new IOException(String.format("Found %s replication strings",
            replicationStrings.size()));
      }
      transitionTo(State.FILE_WITH_REPLICATION);
      currentReplication = Short.parseShort(replicationStrings.get(0));
    }
    Matcher blockMatcher = BLOCK_PATTERN.matcher(line);
    List<BlockInfo> blockInfos = new ArrayList<>();
    while (blockMatcher.find()) {
      if (currentState != State.FILE_WITH_REPLICATION) {
        throw new IOException(
            "Found a block string when in state: " + currentState);
      }
      long id = Long.parseLong(blockMatcher.group(1));
      long gs = Long.parseLong(blockMatcher.group(2));
      long size = Long.parseLong(blockMatcher.group(3));
      blockInfos.add(new BlockInfo(id, gs, size, currentReplication));
    }
    if (line.contains("</inode>")) {
      transitionTo(State.INODE_SECTION);
    }
    if (line.contains("</INodeSection>")) {
      transitionTo(State.DEFAULT);
    }
    return blockInfos;
  }

  /**
   * Attempt to transition to another state.
   *
   * @param nextState The new state to transition to.
   * @throws IOException If the transition from the current state to
   *                     {@code nextState} is not allowed.
   */
  private void transitionTo(State nextState) throws IOException {
    if (currentState.transitionAllowed(nextState)) {
      currentState = nextState;
    } else {
      throw new IOException("State transition not allowed; from " + currentState
          + " to " + nextState);
    }
  }

  /**
   * @param xml An XML string
   * @param field The field whose value(s) should be extracted
   * @return List of the field's values.
   */
  private static List<String> valuesFromXMLString(String xml, String field) {
    Matcher m = Pattern.compile("<" + field + ">(.+?)</" + field + ">")
        .matcher(xml);
    List<String> found = new ArrayList<>();
    while (m.find()) {
      found.add(m.group(1));
    }
    return found;
  }

  private enum State {
    DEFAULT,
    INODE_SECTION,
    INODE,
    FILE,
    FILE_WITH_REPLICATION;

    private final Set<State> allowedTransitions = new HashSet<>();
    static {
      DEFAULT.addTransitions(DEFAULT, INODE_SECTION);
      INODE_SECTION.addTransitions(DEFAULT, INODE);
      INODE.addTransitions(INODE_SECTION, FILE);
      FILE.addTransitions(INODE_SECTION, FILE_WITH_REPLICATION);
      FILE_WITH_REPLICATION.addTransitions(INODE_SECTION);
    }

    private void addTransitions(State... nextState) {
      allowedTransitions.addAll(Arrays.asList(nextState));
    }

    boolean transitionAllowed(State nextState) {
      return allowedTransitions.contains(nextState);
    }
  }

}
