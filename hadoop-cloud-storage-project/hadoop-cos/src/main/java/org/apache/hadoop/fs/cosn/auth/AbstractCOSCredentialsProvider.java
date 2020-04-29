/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.cosn.auth;

import com.qcloud.cos.auth.COSCredentialsProvider;
import org.apache.hadoop.conf.Configuration;

import javax.annotation.Nullable;
import java.net.URI;

/**
 * The base class for COS credential providers which take a URI or
 * configuration in their constructor.
 */
public abstract class AbstractCOSCredentialsProvider
    implements COSCredentialsProvider {
  private final URI uri;
  private final Configuration conf;

  public AbstractCOSCredentialsProvider(@Nullable URI uri,
                                        Configuration conf) {
    this.uri = uri;
    this.conf = conf;
  }

  public URI getUri() {
    return uri;
  }

  public Configuration getConf() {
    return conf;
  }
}