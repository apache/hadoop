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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import org.junit.Test;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerUtilities.trimQueueName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link FairSchedulerUtilities}.
 */
public class TestFairSchedulerUtilities {

  @Test
  public void testTrimQueueNameEquals() throws Exception {
    final String[] equalsStrings = {
        // no spaces
        "a",
        // leading spaces
        " a",
        " \u3000a",
        "\u2002\u3000\r\u0085\u200A\u2005\u2000\u3000a",
        "\u2029\u000B\u3000\u2008\u2003\u205F\u3000\u1680a",
        "\u0009\u0020\u2006\u2001\u202F\u00A0\u000C\u2009a",
        "\u3000\u2004\u3000\u3000\u2028\n\u2007\u3000a",
        // trailing spaces
        "a\u200A",
        "a  \u0085 ",
        // spaces on both sides
        " a ",
        "  a\u00A0",
        "\u0009\u0020\u2006\u2001\u202F\u00A0\u000C\u2009a" +
            "\u3000\u2004\u3000\u3000\u2028\n\u2007\u3000",
    };
    for (String s : equalsStrings) {
      assertEquals("a", trimQueueName(s));
    }
  }

  @Test
  public void testTrimQueueNamesEmpty() throws Exception {
    assertNull(trimQueueName(null));
    final String spaces = "\u2002\u3000\r\u0085\u200A\u2005\u2000\u3000"
        + "\u2029\u000B\u3000\u2008\u2003\u205F\u3000\u1680"
        + "\u0009\u0020\u2006\u2001\u202F\u00A0\u000C\u2009"
        + "\u3000\u2004\u3000\u3000\u2028\n\u2007\u3000";
    assertTrue(trimQueueName(spaces).isEmpty());
  }
}
