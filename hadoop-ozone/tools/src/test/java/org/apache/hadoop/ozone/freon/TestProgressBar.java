/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.freon;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.PrintStream;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
/**
 * Tests for the Progressbar class for Freon.
 */
public class TestProgressBar {

  private PrintStream stream;

  @Before
  public void setupMock() {
    stream = mock(PrintStream.class);
  }

  @Test
  public void testWithRunnable() {

    int maxValue = 10;

    ProgressBar progressbar = new ProgressBar(stream, maxValue);

    Runnable task = () -> {
      IntStream.range(0, maxValue).forEach(
          counter -> {
            progressbar.incrementProgress();
          }
      );
    };

    progressbar.start(task);

    verify(stream, atLeastOnce()).print(anyChar());
    verify(stream, atLeastOnce()).print(anyString());
  }

  @Test
  public void testWithSupplier() {

    int maxValue = 10;

    ProgressBar progressbar = new ProgressBar(stream, maxValue);

    Supplier<Long> tasks = () -> {
      IntStream.range(0, maxValue).forEach(
          counter -> {
            progressbar.incrementProgress();
          }
      );

      return 1L; //return the result of the dummy task
    };

    progressbar.start(tasks);

    verify(stream, atLeastOnce()).print(anyChar());
    verify(stream, atLeastOnce()).print(anyString());
  }

  @Test
  public void testWithFunction() {

    int maxValue = 10;
    Long result;

    ProgressBar progressbar = new ProgressBar(stream, maxValue);

    Function<Long, String> task = (Long l) -> {
      IntStream.range(0, maxValue).forEach(
          counter -> {
            progressbar.incrementProgress();
          }
      );

      return "dummy result"; //return the result of the dummy task
    };

    progressbar.start(1L, task);

    verify(stream, atLeastOnce()).print(anyChar());
    verify(stream, atLeastOnce()).print(anyString());
  }

}
