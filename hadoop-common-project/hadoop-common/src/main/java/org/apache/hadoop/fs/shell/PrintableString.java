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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * The {code PrintableString} class converts any string to a printable string
 * by replacing non-printable characters with ?.
 *
 * Categories of Unicode non-printable characters:
 * <ul>
 * <li> Control characters   (Cc)
 * <li> Formatting Unicode   (Cf)
 * <li> Private use Unicode  (Co)
 * <li> Unassigned Unicode   (Cn)
 * <li> Standalone surrogate (Unfortunately no matching Unicode category)
 * </ul>
 *
 * @see Character
 * @see <a href="http://www.unicode.org/">The Unicode Consortium</a>
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
class PrintableString {
  private static final char REPLACEMENT_CHAR = '?';

  private final String printableString;

  PrintableString(String rawString) {
    StringBuilder stringBuilder = new StringBuilder(rawString.length());
    for (int offset = 0; offset < rawString.length();) {
      int codePoint = rawString.codePointAt(offset);
      offset += Character.charCount(codePoint);

      switch (Character.getType(codePoint)) {
      case Character.CONTROL:     // Cc
      case Character.FORMAT:      // Cf
      case Character.PRIVATE_USE: // Co
      case Character.SURROGATE:   // Cs
      case Character.UNASSIGNED:  // Cn
        stringBuilder.append(REPLACEMENT_CHAR);
        break;
      default:
        stringBuilder.append(Character.toChars(codePoint));
        break;
      }
    }
    printableString = stringBuilder.toString();
  }

  public String toString() {
    return printableString;
  }
}
