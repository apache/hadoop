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
package org.apache.hadoop.chukwa.util;

import junit.framework.TestCase;

public class TestRecordConsts extends TestCase {

  public void testEscapeAllButLastRecordSeparator()
  {
    String post = RecordConstants.escapeAllButLastRecordSeparator("\n", "foo bar baz\n");
    assertEquals(post, "foo bar baz\n");
    
    post = RecordConstants.escapeAllButLastRecordSeparator("\n", "foo\nbar\nbaz\n");
    post = post.replaceAll(RecordConstants.RECORD_SEPARATOR_ESCAPE_SEQ, "^D");
    assertEquals(post, "foo^D\nbar^D\nbaz\n");

    System.out.println("string is " + post+".");
  }

  public void testEscapeAllRecordSeparators()
  {
  }

}
