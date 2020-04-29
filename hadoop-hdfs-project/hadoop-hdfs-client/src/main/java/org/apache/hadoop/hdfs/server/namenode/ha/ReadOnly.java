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
package org.apache.hadoop.hdfs.server.namenode.ha;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Marker interface used to annotate methods that are readonly.
 */
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@InterfaceAudience.Private
@InterfaceStability.Evolving
public @interface ReadOnly {
  /**
   * @return if true, the annotated method may update the last accessed time
   * while performing its read, if access time is enabled.
   */
  boolean atimeAffected() default false;

  /**
   * @return if true, the target method should only be invoked on the active
   * namenode. This applies to operations that need to access information that
   * is only available on the active namenode.
   */
  boolean activeOnly() default false;

  /**
   * @return if true, when processing the rpc call of the target method, the
   * server side will wait if server state id is behind client (msync). If
   * false, the method will be processed regardless of server side state.
   */
  boolean isCoordinated() default false;
}
