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

package org.apache.hadoop.yarn.util;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import java.util.regex.Pattern;

import org.apache.hadoop.classification.InterfaceAudience.Private;

/**
 * Common string manipulation helpers
 */
@Private
public final class StringHelper {
  // Common joiners to avoid per join creation of joiners
  public static final Joiner SSV_JOINER = Joiner.on(' ');
  public static final Joiner CSV_JOINER = Joiner.on(',');
  public static final Joiner JOINER = Joiner.on("");
  public static final Joiner _JOINER = Joiner.on('_');
  public static final Joiner PATH_JOINER = Joiner.on('/');
  public static final Joiner PATH_ARG_JOINER = Joiner.on("/:");
  public static final Joiner DOT_JOINER = Joiner.on('.');
  public static final Splitter SSV_SPLITTER =
      Splitter.on(' ').omitEmptyStrings().trimResults();
  public static final Splitter _SPLITTER = Splitter.on('_').trimResults();
  private static final Pattern ABS_URL_RE =Pattern.compile("^(?:\\w+:)?//");

  /**
   * Join on space.
   * @param args to join
   * @return args joined by space
   */
  public static String sjoin(Object... args) {
    return SSV_JOINER.join(args);
  }

  /**
   * Join on comma.
   * @param args to join
   * @return args joined by comma
   */
  public static String cjoin(Object... args) {
    return CSV_JOINER.join(args);
  }

  /**
   * Join on dot
   * @param args to join
   * @return args joined by dot
   */
  public static String djoin(Object... args) {
    return DOT_JOINER.join(args);
  }

  /**
   * Join on underscore
   * @param args to join
   * @return args joined underscore
   */
  public static String _join(Object... args) {
    return _JOINER.join(args);
  }

  /**
   * Join on slash
   * @param args to join
   * @return args joined with slash
   */
  public static String pjoin(Object... args) {
    return PATH_JOINER.join(args);
  }

  /**
   * Join on slash & colon (e.g., path args in routing spec)
   * @param args to join
   * @return args joined with /:
   */
  public static String pajoin(Object... args) {
    return PATH_ARG_JOINER.join(args);
  }

  /**
   * Join without separator
   * @param args
   * @return joined args with no separator
   */
  public static String join(Object... args) {
    return JOINER.join(args);
  }

  /**
   * Join with a separator
   * @param sep the separator
   * @param args to join
   * @return args joined with a separator
   */
  public static String joins(String sep, Object...args) {
    return Joiner.on(sep).join(args);
  }

  /**
   * Split on space & trim results.
   * @param s the string to split
   * @return an iterable of strings
   */
  public static Iterable<String> split(CharSequence s) {
    return SSV_SPLITTER.split(s);
  }

  /**
   * Split on _ & trim results
   * @param s the string to split
   * @return an iterable of strings
   */
  public static Iterable<String> _split(CharSequence s) {
    return _SPLITTER.split(s);
  }

  /**
   * Check whether a url is absolute or note
   * @param url to check
   * @return true if url starts with scheme:// or //
   */
  public static boolean isAbsUrl(CharSequence url) {
    return ABS_URL_RE.matcher(url).find();
  }

  /**
   * Join url components
   * @param pathPrefix for relative urls
   * @param args url components to join
   * @return an url string
   */
  public static String ujoin(String pathPrefix, String... args) {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (String part : args) {
      if (first) {
        first = false;
        if (part.startsWith("#") || isAbsUrl(part)) {
          sb.append(part);
        } else {
          uappend(sb, pathPrefix);
          uappend(sb, part);
        }
      } else {
        uappend(sb, part);
      }
    }
    return sb.toString();
  }
  
  private static void uappend(StringBuilder sb, String part) {
    if((sb.length() <= 0 || sb.charAt(sb.length() - 1) != '/') 
        && !part.startsWith("/")) {
      sb.append('/');
    }
    sb.append(part);
  }
  
  public static String percent(double value) {
    return String.format("%.2f", value * 100);
  }

}
