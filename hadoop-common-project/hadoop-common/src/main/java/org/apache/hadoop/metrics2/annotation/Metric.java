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

package org.apache.hadoop.metrics2.annotation;

import java.lang.annotation.*;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Annotation interface for a single metric used to annotate a field or a method
 * in the class.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
@Documented
@Target({ElementType.FIELD, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface Metric {

  public enum Type {
    DEFAULT, COUNTER, GAUGE, TAG
  }

  /**
   * Shorthand for optional name and description
   * @return {description} or {name, description}
   */
  String[] value() default {};

  /**
   * @return optional description of the metric
   */
  String about() default "";

  /**
   * @return optional sample name for MutableStat/Rate/Rates
   */
  String sampleName() default "Ops";

  /**
   * @return optional value name for MutableStat/Rate/Rates
   */
  String valueName() default "Time";

  /**
   * @return true to create a metric snapshot even if unchanged.
   */
  boolean always() default false;

  /**
   * @return optional type (counter|gauge) of the metric
   */
  Type type() default Type.DEFAULT;
}
