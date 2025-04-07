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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter;

import java.io.File;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(MockitoExtension.class)
public class TestConvertedConfigValidator {
  private static final String CONFIG_DIR_PASSES =
      new File("src/test/resources/cs-validation-pass").getAbsolutePath();
  private static final String CONFIG_DIR_FAIL =
      new File("src/test/resources/cs-validation-fail").getAbsolutePath();

  private ConvertedConfigValidator validator;

  @BeforeEach
  public void setup() {
    QueueMetrics.clearQueueMetrics();
    validator = new ConvertedConfigValidator();
  }

  @AfterEach
  public void after() {
    QueueMetrics.clearQueueMetrics();
  }

  @Test
  public void testValidationPassed() throws Exception {
    validator.validateConvertedConfig(CONFIG_DIR_PASSES);

    // expected: no exception
  }

  @Test
  public void testValidationFails() throws Exception {
    assertThrows(VerificationException.class, ()->{
      validator.validateConvertedConfig(CONFIG_DIR_FAIL);
    });
  }
}
