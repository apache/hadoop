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

package org.apache.hadoop.fs.azurebfs.contracts.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Definitions of Annotations for all types of the validators
 */
@InterfaceStability.Evolving
public class ConfigurationValidationAnnotations {
  /**
   * Describes the requirements when validating the annotated int field
   */
  @Target({ ElementType.FIELD })
  @Retention(RetentionPolicy.RUNTIME)
  public @interface IntegerConfigurationValidatorAnnotation {
    String ConfigurationKey();

    int MaxValue() default Integer.MAX_VALUE;

    int MinValue() default Integer.MIN_VALUE;

    int DefaultValue();

    boolean ThrowIfInvalid() default false;
  }

  /**
   * Describes the requirements when validating the annotated long field
   */
  @Target({ ElementType.FIELD })
  @Retention(RetentionPolicy.RUNTIME)
  public @interface LongConfigurationValidatorAnnotation {
    String ConfigurationKey();

    long MaxValue() default Long.MAX_VALUE;

    long MinValue() default Long.MIN_VALUE;

    long DefaultValue();

    boolean ThrowIfInvalid() default false;
  }

  /**
   * Describes the requirements when validating the annotated String field
   */
  @Target({ ElementType.FIELD })
  @Retention(RetentionPolicy.RUNTIME)
  public @interface StringConfigurationValidatorAnnotation {
    String ConfigurationKey();

    String DefaultValue();

    boolean ThrowIfInvalid() default false;
  }

  /**
   * Describes the requirements when validating the annotated String field
   */
  @Target({ ElementType.FIELD })
  @Retention(RetentionPolicy.RUNTIME)
  public @interface Base64StringConfigurationValidatorAnnotation {
    String ConfigurationKey();

    String DefaultValue();

    boolean ThrowIfInvalid() default false;
  }

  /**
   * Describes the requirements when validating the annotated boolean field
   */
  @Target({ ElementType.FIELD })
  @Retention(RetentionPolicy.RUNTIME)
  public @interface BooleanConfigurationValidatorAnnotation {
    String ConfigurationKey();

    boolean DefaultValue();

    boolean ThrowIfInvalid() default false;
  }
}