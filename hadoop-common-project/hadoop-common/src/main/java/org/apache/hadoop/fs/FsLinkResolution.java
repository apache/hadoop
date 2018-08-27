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

package org.apache.hadoop.fs;

import java.io.IOException;

import com.google.common.base.Preconditions;

/**
 * Class to allow Java lambda expressions to be used in {@link FileContext}
 * link resolution.
 * @param <T> type of the returned value.
 */
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
      throws IOException, UnresolvedLinkException {
    return fn.apply(fs, p);
  }

  /**
   * The signature of the function to invoke.
   * @param <T> return type.
   * @throws UnresolvedLinkException link resolution failure
   * @throws IOException other IO failure.
   */
  @FunctionalInterface
  public interface FsLinkResolutionFunction<T> {
    T apply(final AbstractFileSystem fs, final Path p)
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
    return new FsLinkResolution<T>(fn).resolve(fileContext, path);
  }
}
