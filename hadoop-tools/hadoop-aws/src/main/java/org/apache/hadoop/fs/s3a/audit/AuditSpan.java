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

package org.apache.hadoop.fs.s3a.audit;

import java.io.Closeable;
import java.io.IOException;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.Request;
import com.amazonaws.Response;
import com.amazonaws.SdkBaseException;

import org.apache.hadoop.fs.s3a.Retries;

/**
 * This is a span created by an {@link OperationAuditor}.
 */
public interface AuditSpan extends Closeable, AuditSpanCallbacks {

  /**
   * Make this span active in the current thread.
   * @return the activated span.
   * This is makes it easy to use in try with resources
   */
  default AuditSpan activate() {
    return this;
  }

  /**
   * Deactivate the span in the current thread.
   */
  void deactivate();

  /**
   * Close calls {@link #deactivate()}; subclasses may override
   * but the audit manager's wrapping span will always relay to
   * {@link #deactivate()} rather
   * than call this method on the wrapped span.
   */
  default void close() {
    deactivate();
  }

  /**
   * Is the span valid? False == this is a span to indicate unbonded.
   * @return true if this span represents a real operation.
   */
  default boolean isValidSpan() {
    return true;
  }
}
