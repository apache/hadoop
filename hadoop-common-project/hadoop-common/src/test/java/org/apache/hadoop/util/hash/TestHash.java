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
package org.apache.hadoop.util.hash;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

public class TestHash {
  static final String LINE = "34563@45kjkksdf/ljfdb9d8fbusd*89uggjsk<dfgjsdfh@sddc2q3esc";

  @Test
  public void testHash() {
    int iterations = 30;
    assertTrue(Hash.JENKINS_HASH == Hash.parseHashType("jenkins"),
        "testHash jenkins error !!!");
    assertTrue(Hash.MURMUR_HASH == Hash.parseHashType("murmur"),
        "testHash murmur error !!!");
    assertTrue(Hash.INVALID_HASH == Hash.parseHashType("undefined"), "testHash undefined");

    Configuration cfg = new Configuration();
    cfg.set("hadoop.util.hash.type", "murmur");
    assertTrue(MurmurHash.getInstance() == Hash.getInstance(cfg), "testHash");

    cfg = new Configuration();
    cfg.set("hadoop.util.hash.type", "jenkins");
    assertTrue(JenkinsHash.getInstance() == Hash.getInstance(cfg),
        "testHash jenkins configuration error !!!");

    cfg = new Configuration();
    assertTrue(MurmurHash.getInstance() == Hash.getInstance(cfg),
        "testHash undefine configuration error !!!");

    assertTrue(JenkinsHash.getInstance() == Hash.getInstance(Hash.JENKINS_HASH),
        "testHash error jenkin getInstance !!!");
    assertTrue(MurmurHash.getInstance() == Hash.getInstance(Hash.MURMUR_HASH),
        "testHash error murmur getInstance !!!");

    assertNull(Hash.getInstance(Hash.INVALID_HASH),
        "testHash error invalid getInstance !!!");

    int murmurHash = Hash.getInstance(Hash.MURMUR_HASH).hash(LINE.getBytes());
    for (int i = 0; i < iterations; i++) {
      assertTrue(murmurHash == Hash.getInstance(Hash.MURMUR_HASH)
          .hash(LINE.getBytes()), "multiple evaluation murmur hash error !!!");
    }

    murmurHash = Hash.getInstance(Hash.MURMUR_HASH).hash(LINE.getBytes(), 67);
    for (int i = 0; i < iterations; i++) {
      assertTrue(murmurHash == Hash.getInstance(Hash.MURMUR_HASH).hash(
          LINE.getBytes(), 67), "multiple evaluation murmur hash error !!!");
    }

    int jenkinsHash = Hash.getInstance(Hash.JENKINS_HASH).hash(LINE.getBytes());
    for (int i = 0; i < iterations; i++) {
      assertTrue(jenkinsHash == Hash.getInstance(Hash.JENKINS_HASH).hash(
          LINE.getBytes()), "multiple evaluation jenkins hash error !!!");
    }

    jenkinsHash = Hash.getInstance(Hash.JENKINS_HASH).hash(LINE.getBytes(), 67);
    for (int i = 0; i < iterations; i++) {
      assertTrue(jenkinsHash == Hash.getInstance(Hash.JENKINS_HASH).hash(
          LINE.getBytes(), 67), "multiple evaluation jenkins hash error !!!");
    }   
  } 
}
