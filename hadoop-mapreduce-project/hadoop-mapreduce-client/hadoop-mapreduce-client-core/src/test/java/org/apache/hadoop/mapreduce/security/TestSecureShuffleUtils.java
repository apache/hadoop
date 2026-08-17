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
package org.apache.hadoop.mapreduce.security;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;

import javax.crypto.SecretKey;

import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.junit.jupiter.api.Test;

public class TestSecureShuffleUtils {

  private static final SecretKey KEY =
      JobTokenSecretManager.createSecretKey(new byte[] {1, 2, 3, 4});

  @Test
  public void testVerifyReplyAcceptsMatchingHash() throws IOException {
    String msg = "1080/mapOutput?job=job_1&reduce=0&map=attempt_0";
    String hash = SecureShuffleUtils.hashFromString(msg, KEY);
    // correct hash must verify without throwing
    SecureShuffleUtils.verifyReply(hash, msg, KEY);
  }

  @Test
  public void testVerifyReplyRejectsTamperedHash() throws IOException {
    String msg = "1080/mapOutput?job=job_1&reduce=0&map=attempt_0";
    String hash = SecureShuffleUtils.hashFromString(msg, KEY);
    // flip the last character of the base64 hash so it no longer matches
    char last = hash.charAt(hash.length() - 1);
    String tampered = hash.substring(0, hash.length() - 1)
        + (last == 'A' ? 'B' : 'A');
    assertThrows(IOException.class,
        () -> SecureShuffleUtils.verifyReply(tampered, msg, KEY));
  }

  @Test
  public void testVerifyReplyRejectsWrongLengthHash() throws IOException {
    String msg = "1080/mapOutput?job=job_1&reduce=0&map=attempt_0";
    String hash = SecureShuffleUtils.hashFromString(msg, KEY);
    // a truncated hash must be rejected, not accepted as a prefix match
    String truncated = hash.substring(0, hash.length() / 2);
    assertThrows(IOException.class,
        () -> SecureShuffleUtils.verifyReply(truncated, msg, KEY));
  }
}
