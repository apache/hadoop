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
package org.apache.hadoop.hdfs.server.common;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.hdfs.protocol.Block;

/**
 * An abstract class used to read and write block maps for provided blocks.
 */
public abstract class BlockFormat<T extends BlockAlias>  {

  /**
   * An abstract class that is used to read {@link BlockAlias}es
   * for provided blocks.
   */
  public static abstract class Reader<U extends BlockAlias>
      implements Iterable<U>, Closeable {

    /**
     * reader options.
     */
    public interface Options { }

    public abstract U resolve(Block ident) throws IOException;

  }

  /**
   * Returns the reader for the provided block map.
   * @param opts reader options
   * @return {@link Reader} to the block map.
   * @throws IOException
   */
  public abstract Reader<T> getReader(Reader.Options opts) throws IOException;

  /**
   * An abstract class used as a writer for the provided block map.
   */
  public static abstract class Writer<U extends BlockAlias>
      implements Closeable {
    /**
     * writer options.
     */
    public interface Options { }

    public abstract void store(U token) throws IOException;

  }

  /**
   * Returns the writer for the provided block map.
   * @param opts writer options.
   * @return {@link Writer} to the block map.
   * @throws IOException
   */
  public abstract Writer<T> getWriter(Writer.Options opts) throws IOException;

  /**
   * Refresh based on the underlying block map.
   * @throws IOException
   */
  public abstract void refresh() throws IOException;

}
