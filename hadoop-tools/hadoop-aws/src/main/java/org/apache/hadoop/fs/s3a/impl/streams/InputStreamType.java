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

package org.apache.hadoop.fs.s3a.impl.streams;

import java.util.function.Function;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.prefetch.PrefetchingInputStreamFactory;

import static org.apache.hadoop.fs.s3a.Constants.INPUT_STREAM_TYPE;
import static org.apache.hadoop.fs.s3a.impl.streams.StreamIntegration.loadCustomFactory;

/**
 * Enum of input stream types.
 * <p>
 * Each enum value contains the factory function actually used to create
 * the factory.
 */
public enum InputStreamType {

  /**
   * The classic input stream.
   */
  Classic(StreamIntegration.CLASSIC, 1, c ->
      new ClassicObjectInputStreamFactory()),

  /**
   * The prefetching input stream.
   */
  Prefetch(StreamIntegration.PREFETCH, 2, c ->
      new PrefetchingInputStreamFactory()),
  /**
   * The analytics input stream.
   */
  Analytics(StreamIntegration.ANALYTICS, 3, c ->
      new AnalyticsStreamFactory()),

  /**
   * The a custom input stream.
   */
  Custom(StreamIntegration.CUSTOM, 4, c -> {
    return loadCustomFactory(c);
  });

  /**
   * Name.
   */
  private final String name;

  /**
   * Stream ID.
   */
  private final int streamID;

  /**
   * Factory lambda-expression.
   */
  private final Function<Configuration, ObjectInputStreamFactory> factory;

  /**
   * String name.
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * Constructor.
   * @param name name, used in configuration binding and capability.
   * @param id ID
   * @param factory factory factory function. "metafactory", really.
   */
  InputStreamType(final String name,
      final int id,
      final Function<Configuration, ObjectInputStreamFactory> factory) {
    this.name = name;
    this.streamID = id;
    this.factory = factory;
  }

  /**
   * Get the ID of this stream.
   * Isolated from the enum ID in case it ever needs to be tuned.
   * @return the numeric ID of the stream.
   */
  public int streamID() {
    return streamID;
  }

  /**
   * Get the capability string for this stream type.
   * @return the name of a string to probe for.
   */
  public String capability() {
    return INPUT_STREAM_TYPE + "." + getName();
  }

  /**
   * Factory constructor.
   * @return the factory function associated with this stream type.
   */
  public Function<Configuration, ObjectInputStreamFactory> factory() {
    return factory;
  }

}
