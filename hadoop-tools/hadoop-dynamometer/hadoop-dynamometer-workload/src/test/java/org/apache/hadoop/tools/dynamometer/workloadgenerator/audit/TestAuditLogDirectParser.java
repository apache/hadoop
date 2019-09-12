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

import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Tests for {@link AuditLogDirectParser}. */
public class TestAuditLogDirectParser {

  private static final long START_TIMESTAMP = 10000;
  private AuditLogDirectParser parser;

  @Before
  public void setup() throws Exception {
    parser = new AuditLogDirectParser();
    Configuration conf = new Configuration();
    conf.setLong(AuditLogDirectParser.AUDIT_START_TIMESTAMP_KEY,
        START_TIMESTAMP);
    parser.initialize(conf);
  }

  private Text getAuditString(String timestamp, String ugi, String cmd,
      String src, String dst) {
    return new Text(
        String.format("%s INFO FSNamesystem.audit: "
                + "allowed=true\tugi=%s\tip=0.0.0.0\tcmd=%s\tsrc=%s\t"
                + "dst=%s\tperm=null\tproto=rpc",
            timestamp, ugi, cmd, src, dst));
  }

  @Test
  public void testSimpleInput() throws Exception {
    Text in = getAuditString("1970-01-01 00:00:11,000", "fakeUser",
        "listStatus", "sourcePath", "null");
    AuditReplayCommand expected = new AuditReplayCommand(1000, "fakeUser",
        "listStatus", "sourcePath", "null", "0.0.0.0");
    assertEquals(expected, parser.parse(in, Function.identity()));
  }

  @Test
  public void testInputWithEquals() throws Exception {
    Text in = getAuditString("1970-01-01 00:00:11,000", "fakeUser",
            "listStatus", "day=1970", "null");
    AuditReplayCommand expected = new AuditReplayCommand(1000, "fakeUser",
            "listStatus", "day=1970", "null", "0.0.0.0");
    assertEquals(expected, parser.parse(in, Function.identity()));
  }

  @Test
  public void testInputWithRenameOptions() throws Exception {
    Text in = getAuditString("1970-01-01 00:00:11,000", "fakeUser",
        "rename (options=[TO_TRASH])", "sourcePath", "destPath");
    AuditReplayCommand expected = new AuditReplayCommand(1000, "fakeUser",
        "rename (options=[TO_TRASH])", "sourcePath", "destPath", "0.0.0.0");
    assertEquals(expected, parser.parse(in, Function.identity()));
  }

  @Test
  public void testInputWithTokenAuth() throws Exception {
    Text in = getAuditString("1970-01-01 00:00:11,000", "fakeUser (auth:TOKEN)",
        "create", "sourcePath", "null");
    AuditReplayCommand expected = new AuditReplayCommand(1000, "fakeUser",
        "create", "sourcePath", "null", "0.0.0.0");
    assertEquals(expected, parser.parse(in, Function.identity()));
  }

  @Test
  public void testInputWithProxyUser() throws Exception {
    Text in = getAuditString("1970-01-01 00:00:11,000",
        "proxyUser (auth:TOKEN) via fakeUser", "create", "sourcePath", "null");
    AuditReplayCommand expected = new AuditReplayCommand(1000, "proxyUser",
        "create", "sourcePath", "null", "0.0.0.0");
    assertEquals(expected, parser.parse(in, Function.identity()));
  }

  @Test
  public void testParseDefaultDateFormat() throws Exception {
    Text in = getAuditString("1970-01-01 13:00:00,000",
        "ignored", "ignored", "ignored", "ignored");
    AuditReplayCommand expected = new AuditReplayCommand(
        13 * 60 * 60 * 1000 - START_TIMESTAMP,
        "ignored", "ignored", "ignored", "ignored", "0.0.0.0");
    assertEquals(expected, parser.parse(in, Function.identity()));
  }

  @Test
  public void testParseCustomDateFormat() throws Exception {
    parser = new AuditLogDirectParser();
    Configuration conf = new Configuration();
    conf.setLong(AuditLogDirectParser.AUDIT_START_TIMESTAMP_KEY, 0);
    conf.set(AuditLogDirectParser.AUDIT_LOG_DATE_FORMAT_KEY,
        "yyyy-MM-dd hh:mm:ss,SSS a");
    parser.initialize(conf);
    Text in = getAuditString("1970-01-01 01:00:00,000 PM",
        "ignored", "ignored", "ignored", "ignored");
    AuditReplayCommand expected = new AuditReplayCommand(13 * 60 * 60 * 1000,
        "ignored", "ignored", "ignored", "ignored", "0.0.0.0");
    assertEquals(expected, parser.parse(in, Function.identity()));
  }

  @Test
  public void testParseCustomTimeZone() throws Exception {
    parser = new AuditLogDirectParser();
    Configuration conf = new Configuration();
    conf.setLong(AuditLogDirectParser.AUDIT_START_TIMESTAMP_KEY, 0);
    conf.set(AuditLogDirectParser.AUDIT_LOG_DATE_TIME_ZONE_KEY, "Etc/GMT-1");
    parser.initialize(conf);
    Text in = getAuditString("1970-01-01 01:00:00,000",
        "ignored", "ignored", "ignored", "ignored");
    AuditReplayCommand expected = new AuditReplayCommand(0,
        "ignored", "ignored", "ignored", "ignored", "0.0.0.0");
    assertEquals(expected, parser.parse(in, Function.identity()));
  }

  @Test
  public void testParseCustomAuditLineFormat() throws Exception {
    Text auditLine = new Text("CUSTOM FORMAT (1970-01-01 00:00:00,000) "
        + "allowed=true\tugi=fakeUser\tip=0.0.0.0\tcmd=fakeCommand\tsrc=src\t"
        + "dst=null\tperm=null\tproto=rpc");
    parser = new AuditLogDirectParser();
    Configuration conf = new Configuration();
    conf.setLong(AuditLogDirectParser.AUDIT_START_TIMESTAMP_KEY, 0);
    conf.set(AuditLogDirectParser.AUDIT_LOG_PARSE_REGEX_KEY,
        "CUSTOM FORMAT \\((?<timestamp>.+?)\\) (?<message>.+)");
    parser.initialize(conf);
    AuditReplayCommand expected = new AuditReplayCommand(0,
        "fakeUser", "fakeCommand", "src", "null", "0.0.0.0");
    assertEquals(expected, parser.parse(auditLine, Function.identity()));
  }

}
