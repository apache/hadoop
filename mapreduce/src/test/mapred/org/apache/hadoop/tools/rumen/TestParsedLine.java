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
package org.apache.hadoop.tools.rumen;

import org.apache.hadoop.util.StringUtils;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestParsedLine {
  static final char[] CHARS_TO_ESCAPE = new char[]{'=', '"', '.'};
  
  String buildLine(String type, String[] kvseq) {
    StringBuilder sb = new StringBuilder();
    sb.append(type);
    for (int i=0; i<kvseq.length; ++i) {
      sb.append(" ");
      if (kvseq[i].equals(".") || kvseq[i].equals("\n")) {
        sb.append(kvseq[i]);
        continue;
      }
      if (i == kvseq.length-1) {
        fail("Incorrect input, expecting value.");
      }
      sb.append(kvseq[i++]);
      sb.append("=\"");
      sb.append(StringUtils.escapeString(kvseq[i], StringUtils.ESCAPE_CHAR,
          CHARS_TO_ESCAPE));
      sb.append("\"");
    }
    return sb.toString();
  }
  
  void testOneLine(String type, String... kvseq) {
    String line = buildLine(type, kvseq);
    ParsedLine pl = new ParsedLine(line, Hadoop20JHParser.internalVersion);
    assertEquals("Mismatching type", type, pl.getType().toString());
    for (int i = 0; i < kvseq.length; ++i) {
      if (kvseq[i].equals(".") || kvseq[i].equals("\n")) {
        continue;
      }

      assertEquals("Key mismatching for " + kvseq[i], kvseq[i + 1], StringUtils
          .unEscapeString(pl.get(kvseq[i]), StringUtils.ESCAPE_CHAR,
              CHARS_TO_ESCAPE));
      ++i;
    }
  }
  
  @Test
  public void testEscapedQuote() {
    testOneLine("REC", "A", "x", "B", "abc\"de", "C", "f");
    testOneLine("REC", "B", "abcde\"", "C", "f");
    testOneLine("REC", "A", "x", "B", "\"abcde");
  }

  @Test
  public void testEqualSign() {
    testOneLine("REC1", "A", "x", "B", "abc=de", "C", "f");
    testOneLine("REC2", "B", "=abcde", "C", "f");
    testOneLine("REC3", "A", "x", "B", "abcde=");
  }

  @Test
  public void testSpace() {
    testOneLine("REC1", "A", "x", "B", "abc de", "C", "f");
    testOneLine("REC2", "B", " ab c de", "C", "f");
    testOneLine("REC3", "A", "x", "B", "abc\t  de  ");
  }

  @Test
  public void testBackSlash() {
    testOneLine("REC1", "A", "x", "B", "abc\\de", "C", "f");
    testOneLine("REC2", "B", "\\ab\\c\\de", "C", "f");
    testOneLine("REC3", "A", "x", "B", "abc\\\\de\\");
    testOneLine("REC4", "A", "x", "B", "abc\\\"de\\\"", "C", "f");
  }

  @Test
  public void testLineDelimiter() {
    testOneLine("REC1", "A", "x", "B", "abc.de", "C", "f");
    testOneLine("REC2", "B", ".ab.de");
    testOneLine("REC3", "A", "x", "B", "abc.de.");
    testOneLine("REC4", "A", "x", "B", "abc.de", ".");
  }
  
  @Test
  public void testMultipleLines() {
    testOneLine("REC1", "A", "x", "\n", "B", "abc.de", "\n", "C", "f", "\n", ".");
  }
}
