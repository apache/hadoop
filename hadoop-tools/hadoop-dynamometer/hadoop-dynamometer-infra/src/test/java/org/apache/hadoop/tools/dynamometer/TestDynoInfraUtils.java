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
package org.apache.hadoop.tools.dynamometer;

import java.util.Set;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


/** Tests for {@link DynoInfraUtils}. */
public class TestDynoInfraUtils {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestDynoInfraUtils.class);

  @Test
  public void testParseStaleDatanodeListSingleDatanode() throws Exception {
    // Confirm all types of values can be properly parsed
    String json = "{"
        + "\"1.2.3.4:5\": {"
        + "  \"numBlocks\": 5,"
        + "  \"fooString\":\"stringValue\","
        + "  \"fooInteger\": 1,"
        + "  \"fooFloat\": 1.0,"
        + "  \"fooArray\": []"
        + "}"
        + "}";
    Set<String> out = DynoInfraUtils.parseStaleDataNodeList(json, 10, LOG);
    assertEquals(1, out.size());
    assertTrue(out.contains("1.2.3.4:5"));
  }

  @Test
  public void testParseStaleDatanodeListMultipleDatanodes() throws Exception {
    String json = "{"
        + "\"1.2.3.4:1\": {\"numBlocks\": 0}, "
        + "\"1.2.3.4:2\": {\"numBlocks\": 15}, "
        + "\"1.2.3.4:3\": {\"numBlocks\": 5}, "
        + "\"1.2.3.4:4\": {\"numBlocks\": 10} "
        + "}";
    Set<String> out = DynoInfraUtils.parseStaleDataNodeList(json, 10, LOG);
    assertEquals(2, out.size());
    assertTrue(out.contains("1.2.3.4:1"));
    assertTrue(out.contains("1.2.3.4:3"));
  }

}
