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

package org.apache.hadoop.yarn.server.resourcemanager.placement;

import org.junit.Test;

import static org.apache.hadoop.yarn.server.resourcemanager.placement.FairQueuePlacementUtils.DOT;
import static org.apache.hadoop.yarn.server.resourcemanager.placement.FairQueuePlacementUtils.DOT_REPLACEMENT;
import static org.apache.hadoop.yarn.server.resourcemanager.placement.FairQueuePlacementUtils.ROOT_QUEUE;
import static org.apache.hadoop.yarn.server.resourcemanager.placement.FairQueuePlacementUtils.assureRoot;
import static org.apache.hadoop.yarn.server.resourcemanager.placement.FairQueuePlacementUtils.cleanName;
import static org.apache.hadoop.yarn.server.resourcemanager.placement.FairQueuePlacementUtils.isValidQueueName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests of the utility methods from {@link FairQueuePlacementUtils}.
 */
public class TestFairQueuePlacementUtils {

  /**
   * Test name trimming and dot replacement in names.
   */
  @Test
  public void testCleanName() {
    // permutations of dot placements
    final String clean = "clean";
    final String dotted = "not.clean";
    final String multiDot = "more.un.clean";
    final String seqDot = "not..clean";
    final String unTrimmed = " .invalid. "; // not really a valid queue

    String cleaned = cleanName(clean);
    assertEquals("Name was changed and it should not", clean, cleaned);
    cleaned = cleanName(dotted);
    assertFalse("Cleaned name contains dots and it should not",
        cleaned.contains(DOT));
    cleaned = cleanName(multiDot);
    assertFalse("Cleaned name contains dots and it should not",
        cleaned.contains(DOT));
    assertNotEquals("Multi dot failed: wrong replacements found",
        cleaned.indexOf(DOT_REPLACEMENT),
        cleaned.lastIndexOf(DOT_REPLACEMENT));
    cleaned = cleanName(seqDot);
    assertFalse("Cleaned name contains dots and it should not",
        cleaned.contains(DOT));
    assertNotEquals("Sequential dot failed: wrong replacements found",
        cleaned.indexOf(DOT_REPLACEMENT),
        cleaned.lastIndexOf(DOT_REPLACEMENT));
    cleaned = cleanName(unTrimmed);
    assertTrue("Trimming start failed: space not removed or dot not replaced",
        cleaned.startsWith(DOT_REPLACEMENT));
    assertTrue("Trimming end failed: space not removed or dot not replaced",
        cleaned.endsWith(DOT_REPLACEMENT));
  }

  @Test
  public void testAssureRoot() {
    // permutations of rooted queue names
    final String queueName = "base";
    final String rootOnly = "root";
    final String rootNoDot = "rootbase";
    final String alreadyRoot = "root.base";

    String rooted = assureRoot(queueName);
    assertTrue("Queue should have root prefix (base)",
        rooted.startsWith(ROOT_QUEUE + DOT));
    rooted = assureRoot(rootOnly);
    assertEquals("'root' queue should not have root prefix (root)",
        rootOnly, rooted);
    rooted = assureRoot(rootNoDot);
    assertTrue("Queue should have root prefix (rootbase)",
        rooted.startsWith(ROOT_QUEUE + DOT));
    assertEquals("'root' queue base was replaced and not prefixed", 5,
        rooted.lastIndexOf(ROOT_QUEUE));
    rooted = assureRoot(alreadyRoot);
    assertEquals("Root prefixed queue changed and it should not (root.base)",
        rooted, alreadyRoot);
    assertNull("Null queue did not return null queue",
        assureRoot(null));
    assertEquals("Empty queue did not return empty name", "",
        assureRoot(""));
  }

  @Test
  public void testIsValidQueueName() {
    // permutations of valid/invalid names
    final String valid = "valid";
    final String validRooted = "root.valid";
    final String rootOnly = "root";
    final String startDot = ".invalid";
    final String endDot = "invalid.";
    final String startSpace = " invalid";
    final String endSpace = "invalid ";
    final String unicodeSpace = "\u00A0invalid";

    assertFalse("'null' queue was not marked as invalid",
        isValidQueueName(null));
    assertTrue("empty queue was not tagged valid", isValidQueueName(""));
    assertTrue("Simple queue name was not tagged valid (valid)",
        isValidQueueName(valid));
    assertTrue("Root only queue was not tagged valid (root)",
        isValidQueueName(rootOnly));
    assertTrue("Root prefixed queue was not tagged valid (root.valid)",
        isValidQueueName(validRooted));
    assertFalse("Queue starting with dot was not tagged invalid (.invalid)",
        isValidQueueName(startDot));
    assertFalse("Queue ending with dot was not tagged invalid (invalid.)",
        isValidQueueName(endDot));
    assertFalse("Queue starting with space was not tagged invalid ( invalid)",
        isValidQueueName(startSpace));
    assertFalse("Queue ending with space was not tagged invalid (invalid )",
        isValidQueueName(endSpace));
    // just one for sanity check extensive tests are in the scheduler utils
    assertFalse("Queue with unicode space was not tagged as invalid (unicode)",
        isValidQueueName(unicodeSpace));
  }
}