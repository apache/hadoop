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

package org.apache.hadoop.fs.s3a.audit;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Class to help parse AWS S3 Logs.
 * see https://docs.aws.amazon.com/AmazonS3/latest/userguide/LogFormat.html
 *
 * Getting the regexp right is surprisingly hard; this class does it
 * explicitly and names each group in the process.
 * All group names are included in {@link #AWS_LOG_REGEXP_GROUPS} in the order
 * within the log entries.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class S3LogParser {

  private S3LogParser() {
  }

  /**
   * Simple entry: anything up to a space.
   * {@value}.
   */
  private static final String SIMPLE = "[^ ]*";

  /**
   * Date/Time. Everything within square braces.
   * {@value}.
   */
  private static final String DATETIME = "\\[(.*?)\\]";

  /**
   * A natural number or "-".
   * {@value}.
   */
  private static final String NUMBER = "(-|[0-9]*)";

  /**
   * A Quoted field or "-".
   * {@value}.
   */
  private static final String QUOTED = "(-|\"[^\"]*\")";


  /**
   * An entry in the regexp.
   * @param name name of the group
   * @param pattern pattern to use in the regexp
   * @return the pattern for the regexp
   */
  private static String e(String name, String pattern) {
    return String.format("(?<%s>%s) ", name, pattern);
  }

  /**
   * An entry in the regexp.
   * @param name name of the group
   * @param pattern pattern to use in the regexp
   * @return the pattern for the regexp
   */
  private static String eNoTrailing(String name, String pattern) {
    return String.format("(?<%s>%s)", name, pattern);
  }

  /**
   * Simple entry using the {@link #SIMPLE} pattern.
   * @param name name of the element (for code clarity only)
   * @return the pattern for the regexp
   */
  private static String e(String name) {
    return e(name, SIMPLE);
  }

  /**
   * Quoted entry using the {@link #QUOTED} pattern.
   * @param name name of the element (for code clarity only)
   * @return the pattern for the regexp
   */
  private static String q(String name) {
    return e(name, QUOTED);
  }

  /**
   * Log group {@value}.
   */
  public static final String OWNER_GROUP = "owner";

  /**
   * Log group {@value}.
   */
  public static final String BUCKET_GROUP = "bucket";

  /**
   * Log group {@value}.
   */
  public static final String TIMESTAMP_GROUP = "timestamp";

  /**
   * Log group {@value}.
   */
  public static final String REMOTEIP_GROUP = "remoteip";

  /**
   * Log group {@value}.
   */
  public static final String REQUESTER_GROUP = "requester";

  /**
   * Log group {@value}.
   */
  public static final String REQUESTID_GROUP = "requestid";

  /**
   * Log group {@value}.
   */
  public static final String VERB_GROUP = "verb";

  /**
   * Log group {@value}.
   */
  public static final String KEY_GROUP = "key";

  /**
   * Log group {@value}.
   */
  public static final String REQUESTURI_GROUP = "requesturi";

  /**
   * Log group {@value}.
   */
  public static final String HTTP_GROUP = "http";

  /**
   * Log group {@value}.
   */
  public static final String AWSERRORCODE_GROUP = "awserrorcode";

  /**
   * Log group {@value}.
   */
  public static final String BYTESSENT_GROUP = "bytessent";

  /**
   * Log group {@value}.
   */
  public static final String OBJECTSIZE_GROUP = "objectsize";

  /**
   * Log group {@value}.
   */
  public static final String TOTALTIME_GROUP = "totaltime";

  /**
   * Log group {@value}.
   */
  public static final String TURNAROUNDTIME_GROUP = "turnaroundtime";

  /**
   * Log group {@value}.
   */
  public static final String REFERRER_GROUP = "referrer";

  /**
   * Log group {@value}.
   */
  public static final String USERAGENT_GROUP = "useragent";

  /**
   * Log group {@value}.
   */
  public static final String VERSION_GROUP = "version";

  /**
   * Log group {@value}.
   */
  public static final String HOSTID_GROUP = "hostid";

  /**
   * Log group {@value}.
   */
  public static final String SIGV_GROUP = "sigv";

  /**
   * Log group {@value}.
   */
  public static final String CYPHER_GROUP = "cypher";

  /**
   * Log group {@value}.
   */
  public static final String AUTH_GROUP = "auth";

  /**
   * Log group {@value}.
   */
  public static final String ENDPOINT_GROUP = "endpoint";

  /**
   * Log group {@value}.
   */
  public static final String TLS_GROUP = "tls";

  /**
   * This is where anything at the tail of a log
   * entry ends up; it is null unless/until the AWS
   * logs are enhanced in future.
   * Value {@value}.
   */
  public static final String TAIL_GROUP = "tail";

  /**
   * Construct the log entry pattern.
   */
  public static final String LOG_ENTRY_REGEXP = ""
      + e(OWNER_GROUP)
      + e(BUCKET_GROUP)
      + e(TIMESTAMP_GROUP, DATETIME)
      + e(REMOTEIP_GROUP)
      + e(REQUESTER_GROUP)
      + e(REQUESTID_GROUP)
      + e(VERB_GROUP)
      + e(KEY_GROUP)
      + q(REQUESTURI_GROUP)
      + e(HTTP_GROUP, NUMBER)
      + e(AWSERRORCODE_GROUP)
      + e(BYTESSENT_GROUP)
      + e(OBJECTSIZE_GROUP)
      + e(TOTALTIME_GROUP)
      + e(TURNAROUNDTIME_GROUP)
      + q(REFERRER_GROUP)
      + q(USERAGENT_GROUP)
      + e(VERSION_GROUP)
      + e(HOSTID_GROUP)
      + e(SIGV_GROUP)
      + e(CYPHER_GROUP)
      + e(AUTH_GROUP)
      + e(ENDPOINT_GROUP)
      + eNoTrailing(TLS_GROUP, SIMPLE)
      + eNoTrailing(TAIL_GROUP, ".*") // anything which follows
      + "$"; // end of line

  /**
   * Groups in order.
   */
  private static final String[] GROUPS = {
      OWNER_GROUP,
      BUCKET_GROUP,
      TIMESTAMP_GROUP,
      REMOTEIP_GROUP,
      REQUESTER_GROUP,
      REQUESTID_GROUP,
      VERB_GROUP,
      KEY_GROUP,
      REQUESTURI_GROUP,
      HTTP_GROUP,
      AWSERRORCODE_GROUP,
      BYTESSENT_GROUP,
      OBJECTSIZE_GROUP,
      TOTALTIME_GROUP,
      TURNAROUNDTIME_GROUP,
      REFERRER_GROUP,
      USERAGENT_GROUP,
      VERSION_GROUP,
      HOSTID_GROUP,
      SIGV_GROUP,
      CYPHER_GROUP,
      AUTH_GROUP,
      ENDPOINT_GROUP,
      TLS_GROUP,
      TAIL_GROUP
  };

  /**
   * Ordered list of regular expression group names.
   */
  public static final List<String> AWS_LOG_REGEXP_GROUPS =
      Collections.unmodifiableList(Arrays.asList(GROUPS));

  /**
   * And the actual compiled pattern.
   */
  public static final Pattern LOG_ENTRY_PATTERN = Pattern.compile(
      LOG_ENTRY_REGEXP);
}
