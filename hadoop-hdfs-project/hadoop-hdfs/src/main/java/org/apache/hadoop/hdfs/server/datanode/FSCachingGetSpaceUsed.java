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

package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.CachingGetSpaceUsed;
import org.apache.hadoop.fs.GetSpaceUsed;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsVolumeImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fast and accurate class to tell how much space HDFS is using.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class FSCachingGetSpaceUsed extends CachingGetSpaceUsed {
  static final Logger LOG =
      LoggerFactory.getLogger(FSCachingGetSpaceUsed.class);

  public FSCachingGetSpaceUsed(Builder builder) throws IOException {
    super(builder);
  }

  /**
   * The builder class.
   */
  public static class Builder extends GetSpaceUsed.Builder {
    private FsVolumeImpl volume;
    private String bpid;

    public FsVolumeImpl getVolume() {
      return volume;
    }

    public Builder setVolume(FsVolumeImpl fsVolume) {
      this.volume = fsVolume;
      return this;
    }

    public String getBpid() {
      return bpid;
    }

    public Builder setBpid(String bpid) {
      this.bpid = bpid;
      return this;
    }

    @Override
    public GetSpaceUsed build() throws IOException {
      Class clazz = getKlass();
      if (FSCachingGetSpaceUsed.class.isAssignableFrom(clazz)) {
        try {
          setCons(clazz.getConstructor(Builder.class));
        } catch (NoSuchMethodException e) {
          throw new RuntimeException(e);
        }
      }
      return super.build();
    }
  }
}
