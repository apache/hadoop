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

package org.apache.hadoop.fs.shell;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

/**
 * Test {@code PrintableString} class.
 */
public class TestPrintableString {

  private void expect(String reason, String raw, String expected) {
    assertThat(reason, new PrintableString(raw).toString(), is(expected));
  }

  /**
   * Test printable characters.
   */
  @Test
  public void testPrintableCharacters() throws Exception {
    // ASCII
    expect("Should keep ASCII letter", "abcdef237", "abcdef237");
    expect("Should keep ASCII symbol", " !\"|}~", " !\"|}~");

    // Unicode BMP
    expect("Should keep Georgian U+1050 and Box Drawing U+2533",
        "\u1050\u2533--", "\u1050\u2533--");

    // Unicode SMP
    expect("Should keep Linear B U+10000 and Phoenician U+10900",
        "\uD800\uDC00'''\uD802\uDD00", "\uD800\uDC00'''\uD802\uDD00");
  }

  /**
   * Test non-printable characters.
   */
  @Test
  public void testNonPrintableCharacters() throws Exception {
    // Control characters
    expect("Should replace single control character", "abc\rdef", "abc?def");
    expect("Should replace multiple control characters",
        "\babc\tdef", "?abc?def");
    expect("Should replace all control characters", "\f\f\b\n", "????");
    expect("Should replace mixed characters starting with a control",
        "\027ab\0", "?ab?");

    // Formatting Unicode
    expect("Should replace Byte Order Mark", "-\uFEFF--", "-?--");
    expect("Should replace Invisible Separator", "\u2063\t", "??");

    // Private use Unicode
    expect("Should replace private use U+E000", "\uE000", "?");
    expect("Should replace private use U+E123 and U+F432",
        "\uE123abc\uF432", "?abc?");
    expect("Should replace private use in Plane 15 and 16: U+F0000 and " +
        "U+10FFFD, but keep U+1050",
        "x\uDB80\uDC00y\uDBFF\uDFFDz\u1050", "x?y?z\u1050");

    // Unassigned Unicode
    expect("Should replace unassigned U+30000 and U+DFFFF",
        "-\uD880\uDC00-\uDB3F\uDFFF-", "-?-?-");

    // Standalone surrogate character (not in a pair)
    expect("Should replace standalone surrogate U+DB80", "x\uDB80yz", "x?yz");
    expect("Should replace standalone surrogate mixed with valid pair",
        "x\uDB80\uD802\uDD00yz", "x?\uD802\uDD00yz");
  }
}