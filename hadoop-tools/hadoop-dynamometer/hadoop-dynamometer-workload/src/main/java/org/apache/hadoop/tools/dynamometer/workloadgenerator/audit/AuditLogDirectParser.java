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
package org.apache.hadoop.tools.dynamometer.workloadgenerator.audit;

import org.apache.hadoop.thirdparty.com.google.common.base.Splitter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

/**
 * This {@link AuditCommandParser} is used to read commands from an audit log in
 * the original format audit logs are produced in with a standard configuration.
 * It requires setting the {@value AUDIT_START_TIMESTAMP_KEY} configuration to
 * specify what the start time of the audit log was to determine when events
 * occurred relative to this start time.
 * <p>
 * By default, this assumes that the audit log is in the default log format
 * set up by Hadoop, like:
 * <pre>{@code
 *   1970-01-01 00:00:00,000 INFO FSNamesystem.audit: allowed=true ...
 * }</pre>
 * You can adjust this parsing behavior using the various configurations
 * available.
 */
public class AuditLogDirectParser implements AuditCommandParser {

  /** See class Javadoc for more detail. */
  public static final String AUDIT_START_TIMESTAMP_KEY =
      "auditreplay.log-start-time.ms";

  /**
   * The format string used to parse the date which is present in the audit
   * log. This must be a format understood by {@link SimpleDateFormat}.
   */
  public static final String AUDIT_LOG_DATE_FORMAT_KEY =
      "auditreplay.log-date.format";
  public static final String AUDIT_LOG_DATE_FORMAT_DEFAULT =
      "yyyy-MM-dd HH:mm:ss,SSS";

  /**
   * The time zone to use when parsing the audit log timestamp. This must
   * be a format recognized by {@link TimeZone#getTimeZone(String)}.
   */
  public static final String AUDIT_LOG_DATE_TIME_ZONE_KEY =
      "auditreplay.log-date.time-zone";
  public static final String AUDIT_LOG_DATE_TIME_ZONE_DEFAULT = "UTC";

  /**
   * The regex to use when parsing the audit log lines. This should match
   * against a single log line, and create two named capture groups. One
   * must be titled "timestamp" and return a timestamp which can be parsed
   * by a {@link DateFormat date formatter}. The other must be titled "message"
   * and return the audit content, such as "allowed=true ugi=user ...". See
   * {@link #AUDIT_LOG_PARSE_REGEX_DEFAULT} for an example.
   */
  public static final String AUDIT_LOG_PARSE_REGEX_KEY =
      "auditreplay.log-parse-regex";
  public static final String AUDIT_LOG_PARSE_REGEX_DEFAULT =
      "^(?<timestamp>.+?) INFO [^:]+: (?<message>.+)$";

  private static final Splitter SPACE_SPLITTER = Splitter.on(" ").trimResults()
      .omitEmptyStrings();

  private long startTimestamp;
  private DateFormat dateFormat;
  private Pattern logLineParseRegex;

  @Override
  public void initialize(Configuration conf) throws IOException {
    startTimestamp = conf.getLong(AUDIT_START_TIMESTAMP_KEY, -1);
    if (startTimestamp < 0) {
      throw new IOException(
          "Invalid or missing audit start timestamp: " + startTimestamp);
    }
    dateFormat = new SimpleDateFormat(conf.get(AUDIT_LOG_DATE_FORMAT_KEY,
        AUDIT_LOG_DATE_FORMAT_DEFAULT));
    String timeZoneString = conf.get(AUDIT_LOG_DATE_TIME_ZONE_KEY,
        AUDIT_LOG_DATE_TIME_ZONE_DEFAULT);
    dateFormat.setTimeZone(TimeZone.getTimeZone(timeZoneString));
    String logLineParseRegexString =
        conf.get(AUDIT_LOG_PARSE_REGEX_KEY, AUDIT_LOG_PARSE_REGEX_DEFAULT);
    if (!logLineParseRegexString.contains("(?<timestamp>")
        && logLineParseRegexString.contains("(?<message>")) {
      throw new IllegalArgumentException("Must configure regex with named "
          + "capture groups 'timestamp' and 'message'");
    }
    logLineParseRegex = Pattern.compile(logLineParseRegexString);
  }

  @Override
  public AuditReplayCommand parse(Text inputLine,
      Function<Long, Long> relativeToAbsolute) throws IOException {
    Matcher m = logLineParseRegex.matcher(inputLine.toString());
    if (!m.find()) {
      throw new IOException(
          "Unable to find valid message pattern from audit log line: `"
              + inputLine + "` using regex `" + logLineParseRegex + "`");
    }
    long relativeTimestamp;
    try {
      relativeTimestamp = dateFormat.parse(m.group("timestamp")).getTime()
          - startTimestamp;
    } catch (ParseException p) {
      throw new IOException(
          "Exception while parsing timestamp from audit log line: `"
          + inputLine + "`", p);
    }
    // Sanitize the = in the rename options field into a : so we can split on =
    String auditMessageSanitized =
        m.group("message").replace("(options=", "(options:");

    Map<String, String> parameterMap = new HashMap<String, String>();
    String[] auditMessageSanitizedList = auditMessageSanitized.split("\t");

    for (String auditMessage : auditMessageSanitizedList) {
      String[] splitMessage = auditMessage.split("=", 2);
      try {
        parameterMap.put(splitMessage[0], splitMessage[1]);
      } catch (ArrayIndexOutOfBoundsException e) {
        throw new IOException(
            "Exception while parsing a message from audit log line: `"
            + inputLine + "`", e);
      }
    }

    return new AuditReplayCommand(relativeToAbsolute.apply(relativeTimestamp),
        // Split the UGI on space to remove the auth and proxy portions of it
        SPACE_SPLITTER.split(parameterMap.get("ugi")).iterator().next(),
        parameterMap.get("cmd").replace("(options:", "(options="),
        parameterMap.get("src"), parameterMap.get("dst"),
        parameterMap.get("ip"));
  }

}
