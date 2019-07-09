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

import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import static org.junit.Assert.*;


/** Tests for {@link XMLParser}. */
public class TestXMLParser {

  /**
   * Testing whether {@link XMLParser} correctly parses an XML fsimage file into
   * {@link BlockInfo}'s. Note that some files have multiple lines.
   */
  @Test
  public void testBlocksFromLine() throws Exception {
    String[] lines = {
        "<INodeSection><lastInodeId>1"
            + "</lastInodeId><inode><id>2</id><type>FILE</type>"
            + "<name>fake-file</name>"
            + "<replication>3</replication><mtime>3</mtime>"
            + "<atime>4</atime>" + "<perferredBlockSize>5</perferredBlockSize>"
            + "<permission>hdfs:hdfs:rw-------</permission>"
            + "<blocks><block><id>6</id><genstamp>7</genstamp>"
            + "<numBytes>8</numBytes></block>"
            + "<block><id>9</id><genstamp>10</genstamp>"
            + "<numBytes>11</numBytes></block></inode>",
        "<inode><type>DIRECTORY</type></inode>", "<inode><type>FILE</type>",
        "<replication>12</replication>",
        "<blocks><block><id>13</id><genstamp>14</genstamp>"
            + "<numBytes>15</numBytes></block>",
        "</inode>",
        "</INodeSection>"
    };

    short replCount = 0; // This is ignored
    Map<BlockInfo, Short> expectedBlockCount = new HashMap<>();
    expectedBlockCount.put(new BlockInfo(6, 7, 8, replCount), (short) 3);
    expectedBlockCount.put(new BlockInfo(9, 10, 11, replCount), (short) 3);
    expectedBlockCount.put(new BlockInfo(13, 14, 15, replCount), (short) 12);

    final Map<BlockInfo, Short> actualBlockCount = new HashMap<>();
    XMLParser parser = new XMLParser();
    for (String line : lines) {
      for (BlockInfo info : parser.parseLine(line)) {
        actualBlockCount.put(info, info.getReplication());
      }
    }

    for (Map.Entry<BlockInfo, Short> expect : expectedBlockCount.entrySet()) {
      assertEquals(expect.getValue(), actualBlockCount.get(expect.getKey()));
    }
  }

  @Test
  public void testNonInodeSectionIgnored() throws Exception {
    String[] lines = {
        "<INodeSection>",
        "</INodeSection>",
        "<OtherSection>",
        "<inode><id>1</id><type>FILE</type><name>fake-file</name>"
            + "<replication>1</replication>",
        "<blocks><block><id>2</id><genstamp>1</genstamp>"
            + "<numBytes>1</numBytes></block>",
        "</inode>",
        "<replication>3</replication>",
        "</OtherSection>"
    };

    XMLParser parser = new XMLParser();
    for (String line : lines) {
      assertTrue((parser.parseLine(line).isEmpty()));
    }
  }
}
