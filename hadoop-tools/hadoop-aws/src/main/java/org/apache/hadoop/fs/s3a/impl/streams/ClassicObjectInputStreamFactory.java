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

import java.io.IOException;

import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.s3a.S3AInputStream;

import static org.apache.hadoop.util.StringUtils.toLowerCase;

/**
 * Factory of classic {@link S3AInputStream} instances.
 */
public class ClassicObjectInputStreamFactory extends AbstractObjectInputStreamFactory {

  public ClassicObjectInputStreamFactory() {
    super("ClassicObjectInputStreamFactory");
  }

  @Override
  public ObjectInputStream readObject(final ObjectReadParameters parameters)
      throws IOException {
    return new S3AInputStream(parameters);
  }

  @Override
  public boolean hasCapability(final String capability) {

    switch (toLowerCase(capability)) {
    case StreamCapabilities.IOSTATISTICS_CONTEXT:
    case StreamCapabilities.READAHEAD:
    case StreamCapabilities.UNBUFFER:
    case StreamCapabilities.VECTOREDIO:
      return true;
    default:
      return super.hasCapability(capability);
    }
  }

  /**
   * Get the number of background threads required for this factory.
   * @return the count of background threads.
   */
  @Override
  public StreamThreadOptions threadRequirements() {
    return new StreamThreadOptions(0, 0, false, true);
  }

}
