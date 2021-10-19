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

package org.apache.hadoop.test;

import java.util.concurrent.Callable;

import org.assertj.core.description.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extra classes to work with AssertJ.
 * These are kept separate from {@link LambdaTestUtils} so there's
 * no requirement for AssertJ to be on the classpath in that broadly
 * used class.
 */
public final class AssertExtensions {

  private static final Logger LOG =
      LoggerFactory.getLogger(AssertExtensions.class);

  private AssertExtensions() {
  }

  /**
   * A description for AssertJ "describedAs" clauses which evaluates the
   * lambda-expression only on failure. That must return a string
   * or null/"" to be skipped.
   * @param eval lambda expression to invoke
   * @return a description for AssertJ
   */
  public static Description dynamicDescription(Callable<String> eval) {
    return new DynamicDescription(eval);
  }

  private static final class DynamicDescription extends Description {
    private final Callable<String> eval;

    private DynamicDescription(final Callable<String> eval) {
      this.eval = eval;
    }

    @Override
    public String value() {
      try {
        return eval.call();
      } catch (Exception e) {
        LOG.warn("Failed to evaluate description: " + e);
        LOG.debug("Evaluation failure", e);
        // return null so that the description evaluation chain
        // will skip this one
        return null;
      }
    }
  }


}
