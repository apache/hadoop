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

package org.apache.hadoop.mapred.lib;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.test.AbstractHadoopTestBase;
import org.apache.hadoop.test.LambdaTestUtils.VoidCallable;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Tests that {@link TaggedInputSplit} rejects a class named on the wire that
 * does not implement the required type, and does not load that class.
 */
public class TestTaggedInputSplitValidation extends AbstractHadoopTestBase {

  public static final class LoadFlag {

    private static volatile boolean loaded = false;

    static boolean isLoaded() {
      return loaded;
    }

    static void setLoaded(boolean loaded) {
      LoadFlag.loaded = loaded;
    }
  }

  /** Not an InputSplit; records that it was loaded. */
  @SuppressWarnings("unused")
  public static final class Sentinel {

    static {
      LoadFlag.setLoaded(true);
    }

    public Sentinel() {
    }
  }

  private static final String SENTINEL =
      TestTaggedInputSplitValidation.class.getName() + "$Sentinel";

  @BeforeEach
  public void resetSentinel() {
    LoadFlag.setLoaded(false);
  }

  private static void assertRejectsWrongKind(VoidCallable action) throws Exception {
    RuntimeException thrown = intercept(RuntimeException.class, action);
    for (Throwable c = thrown; c != null; c = c.getCause()) {
      if (c instanceof ClassCastException) {
        return;
      }
    }
    fail("expected ClassCastException in cause chain of " + thrown, thrown);
  }

  @Test
  public void testRejectsNonInputSplit() throws Exception {
    DataOutputBuffer out = new DataOutputBuffer();
    Text.writeString(out, SENTINEL);
    try (DataInputBuffer in = new DataInputBuffer()) {
      in.reset(out.getData(), out.getLength());
      TaggedInputSplit split = new TaggedInputSplit();
      split.setConf(new Configuration());
      assertRejectsWrongKind(() -> split.readFields(in));
    }
    assertThat(LoadFlag.isLoaded()).
        describedAs("sentinel must not be loaded")
        .isFalse();
  }
}
