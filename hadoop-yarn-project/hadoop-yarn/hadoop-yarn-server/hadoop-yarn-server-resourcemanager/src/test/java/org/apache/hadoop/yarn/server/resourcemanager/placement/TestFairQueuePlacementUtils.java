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

import org.junit.jupiter.api.Test;

import static org.apache.hadoop.yarn.server.resourcemanager.placement.FairQueuePlacementUtils.DOT;
import static org.apache.hadoop.yarn.server.resourcemanager.placement.FairQueuePlacementUtils.DOT_REPLACEMENT;
import static org.apache.hadoop.yarn.server.resourcemanager.placement.FairQueuePlacementUtils.ROOT_QUEUE;
import static org.apache.hadoop.yarn.server.resourcemanager.placement.FairQueuePlacementUtils.assureRoot;
import static org.apache.hadoop.yarn.server.resourcemanager.placement.FairQueuePlacementUtils.cleanName;
import static org.apache.hadoop.yarn.server.resourcemanager.placement.FairQueuePlacementUtils.isValidQueueName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    assertEquals(clean, cleaned, "Name was changed and it should not");
    cleaned = cleanName(dotted);
    assertFalse(cleaned.contains(DOT),
        "Cleaned name contains dots and it should not");
    cleaned = cleanName(multiDot);
    assertFalse(cleaned.contains(DOT),
        "Cleaned name contains dots and it should not");
    assertNotEquals(cleaned.indexOf(DOT_REPLACEMENT),
        cleaned.lastIndexOf(DOT_REPLACEMENT),
        "Multi dot failed: wrong replacements found");
    cleaned = cleanName(seqDot);
    assertFalse(cleaned.contains(DOT),
        "Cleaned name contains dots and it should not");
    assertNotEquals(cleaned.indexOf(DOT_REPLACEMENT),
        cleaned.lastIndexOf(DOT_REPLACEMENT),
        "Sequential dot failed: wrong replacements found");
    cleaned = cleanName(unTrimmed);
    assertTrue(cleaned.startsWith(DOT_REPLACEMENT),
        "Trimming start failed: space not removed or dot not replaced");
    assertTrue(cleaned.endsWith(DOT_REPLACEMENT),
        "Trimming end failed: space not removed or dot not replaced");
  }

  @Test
  public void testAssureRoot() {
    // permutations of rooted queue names
    final String queueName = "base";
    final String rootOnly = "root";
    final String rootNoDot = "rootbase";
    final String alreadyRoot = "root.base";

    String rooted = assureRoot(queueName);
    assertTrue(rooted.startsWith(ROOT_QUEUE + DOT),
        "Queue should have root prefix (base)");
    rooted = assureRoot(rootOnly);
    assertEquals(rootOnly, rooted,
        "'root' queue should not have root prefix (root)");
    rooted = assureRoot(rootNoDot);
    assertTrue(rooted.startsWith(ROOT_QUEUE + DOT),
        "Queue should have root prefix (rootbase)");
    assertEquals(5, rooted.lastIndexOf(ROOT_QUEUE),
        "'root' queue base was replaced and not prefixed");
    rooted = assureRoot(alreadyRoot);
    assertEquals(rooted, alreadyRoot,
        "Root prefixed queue changed and it should not (root.base)");
    assertNull(assureRoot(null), "Null queue did not return null queue");
    assertEquals("", assureRoot(""),
        "Empty queue did not return empty name");
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

    assertFalse(isValidQueueName(null), "'null' queue was not marked as invalid");
    assertTrue(isValidQueueName(""), "empty queue was not tagged valid");
    assertTrue(isValidQueueName(valid),
        "Simple queue name was not tagged valid (valid)");
    assertTrue(isValidQueueName(rootOnly),
        "Root only queue was not tagged valid (root)");
    assertTrue(isValidQueueName(validRooted),
        "Root prefixed queue was not tagged valid (root.valid)");
    assertFalse(isValidQueueName(startDot),
        "Queue starting with dot was not tagged invalid (.invalid)");
    assertFalse(isValidQueueName(endDot),
        "Queue ending with dot was not tagged invalid (invalid.)");
    assertFalse(isValidQueueName(startSpace),
        "Queue starting with space was not tagged invalid ( invalid)");
    assertFalse(isValidQueueName(endSpace),
        "Queue ending with space was not tagged invalid (invalid )");
    // just one for sanity check extensive tests are in the scheduler utils
    assertFalse(isValidQueueName(unicodeSpace),
        "Queue with unicode space was not tagged as invalid (unicode)");
  }
}