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

package org.apache.hadoop.cli.util;

import org.apache.hadoop.util.Shell;
import java.util.regex.Pattern;

/**
 * Comparator for command line tests that attempts to find a regexp
 * within the entire text returned by a command.
 *
 * This comparator differs from RegexpComparator in that it attempts
 * to match the pattern within all of the text returned by the command,
 * rather than matching against each line of the returned text.  This
 * allows matching against patterns that span multiple lines.
 */
public class RegexpAcrossOutputComparator extends ComparatorBase {

  @Override
  public boolean compare(String actual, String expected) {
    if (Shell.WINDOWS) {
      actual = actual.replaceAll("\\r", "");
      expected = expected.replaceAll("\\r", "");
    }
    return Pattern.compile(expected).matcher(actual).find();
  }

}
