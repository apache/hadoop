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

package org.apache.hadoop.test.tags;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.junit.jupiter.api.Tag;

/**
 * JUnit 5 tag to indicate that a test is an integration test which is
 * to be executed against a running service -a service whose
 * deployment and operation is not part of the codebase.
 * <p>
 * This is primarily for test suites which are targeted at
 * remote cloud stores.
 * <p>
 * Key aspects of these tests are not just that they depend upon
 * an external service -the test run must be configured such that
 * it can connect to and authenticate with the service.
 * <p>
 * Consult the documentation of the specific module to
 * determine how to do this.
 *
 * <p> The test runner tag to filter on is {@code integration}.
 */
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Tag("integration")
public @interface IntegrationTest {
}
