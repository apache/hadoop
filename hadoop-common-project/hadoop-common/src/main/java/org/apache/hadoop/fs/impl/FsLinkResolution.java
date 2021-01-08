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

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.FSLinkResolver;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;

/**
 * Class to allow Lambda expressions to be used in {@link FileContext}
 * link resolution.
 * @param <T> type of the returned value.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FsLinkResolution<T> extends FSLinkResolver<T> {

  /**
   * The function to invoke in the {@link #next(AbstractFileSystem, Path)} call.
   */
  private final FsLinkResolutionFunction<T> fn;

  /**
   * Construct an instance with the given function.
   * @param fn function to invoke.
   */
  public FsLinkResolution(final FsLinkResolutionFunction<T> fn) {
    this.fn = Preconditions.checkNotNull(fn);
  }

  @Override
  public T next(final AbstractFileSystem fs, final Path p)
      throws UnresolvedLinkException, IOException {
    return fn.apply(fs, p);
  }

  /**
   * The signature of the function to invoke.
   * @param <T> type resolved to
   */
  @FunctionalInterface
  public interface FsLinkResolutionFunction<T> {

    /**
     *
     * @param fs filesystem to resolve against.
     * @param path path to resolve
     * @return a result of type T
     * @throws UnresolvedLinkException link resolution failure
     * @throws IOException other IO failure.
     */
    T apply(final AbstractFileSystem fs, final Path path)
        throws IOException, UnresolvedLinkException;
  }

  /**
   * Apply the given function to the resolved path under the the supplied
   * FileContext.
   * @param fileContext file context to resolve under
   * @param path path to resolve
   * @param fn function to invoke
   * @param <T> return type.
   * @return the return value of the function as revoked against the resolved
   * path.
   * @throws UnresolvedLinkException link resolution failure
   * @throws IOException other IO failure.
   */
  public static <T> T resolve(
      final FileContext fileContext, final Path path,
      final FsLinkResolutionFunction<T> fn)
      throws UnresolvedLinkException, IOException {
    return new FsLinkResolution<>(fn).resolve(fileContext, path);
  }
}
