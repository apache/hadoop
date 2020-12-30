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

package org.apache.hadoop.fs.impl;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * A wrapper for an IOException which
 * {@link FutureIOSupport#raiseInnerCause(ExecutionException)} knows to
 * always extract the exception.
 *
 * The constructor signature guarantees the cause will be an IOException,
 * and as it checks for a null-argument, non-null.
 * @deprecated use the {@code UncheckedIOException}.
 */
@Deprecated
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class WrappedIOException extends UncheckedIOException {

  private static final long serialVersionUID = 2510210974235779294L;

  /**
   * Construct from a non-null IOException.
   * @param cause inner cause
   * @throws NullPointerException if the cause is null.
   */
  public WrappedIOException(final IOException cause) {
    super(Preconditions.checkNotNull(cause));
  }

  @Override
  public synchronized IOException getCause() {
    return (IOException) super.getCause();
  }
}
