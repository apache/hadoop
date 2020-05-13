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

package org.apache.hadoop.fs.s3a.s3guard;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.S3AFileStatus;

/**
 * Internal iterators.
 */
final class InternalIterators {

  private InternalIterators() {
  }

  /**
   * From a remote status iterator, build a path iterator.
   */
  static final class PathFromRemoteStatusIterator implements
      RemoteIterator<Path> {

    private final RemoteIterator<S3AFileStatus> source;

    /**
     * Construct.
     * @param source source iterator.
     */
    PathFromRemoteStatusIterator(final RemoteIterator<S3AFileStatus> source) {
      this.source = source;
    }

    @Override
    public boolean hasNext() throws IOException {
      return source.hasNext();
    }

    @Override
    public Path next() throws IOException {
      return source.next().getPath();
    }
  }

  /**
   * From a classic java.util.Iterator, build a Hadoop remote iterator.
   * @param <T> type of iterated value.
   */
  static final class RemoteIteratorFromIterator<T> implements
      RemoteIterator<T> {

    private final Iterator<T> source;

    /**
     * Construct.
     * @param source source iterator.
     */
    RemoteIteratorFromIterator(final Iterator<T> source) {
      this.source = source;
    }

    @Override
    public boolean hasNext() {
      return source.hasNext();
    }

    @Override
    public T next() {
      return source.next();
    }
  }

}
