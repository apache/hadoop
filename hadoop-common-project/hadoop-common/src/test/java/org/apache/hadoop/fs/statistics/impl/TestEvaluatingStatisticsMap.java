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

package org.apache.hadoop.fs.statistics.impl;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.assertj.core.api.Assertions.assertThat;

public class TestEvaluatingStatisticsMap extends AbstractHadoopTestBase {


  @Test
  public void testEvaluatingStatisticsMap() {
    EvaluatingStatisticsMap<String> map = new EvaluatingStatisticsMap<>();

    Assertions.assertThat(map).isEmpty();
    Assertions.assertThat(map.keySet()).isEmpty();
    Assertions.assertThat(map.values()).isEmpty();
    Assertions.assertThat(map.entrySet()).isEmpty();

    // fill the map with the environment vars
    final Map<String, String> env = System.getenv();
    env.forEach((k, v) -> map.addFunction(k, any -> v));

    // verify the keys match
    assertThat(map.keySet())
        .describedAs("keys")
        .containsExactlyInAnyOrderElementsOf(env.keySet());

    // and that the values do
    assertThat(map.values())
        .describedAs("Evaluated values")
        .containsExactlyInAnyOrderElementsOf(env.values());

    // now assert that this holds for the entryset.
    env.forEach((k, v) ->
        assertThat(map.get(k))
            .describedAs("looked up key %s", k)
            .isNotNull()
            .isEqualTo(v));

    map.forEach((k, v) ->
        assertThat(env.get(k))
            .describedAs("env var %s", k)
            .isNotNull()
            .isEqualTo(v));


  }
}
