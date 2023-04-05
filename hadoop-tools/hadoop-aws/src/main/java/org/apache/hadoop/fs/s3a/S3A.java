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

package org.apache.hadoop.fs.s3a;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;

import static org.apache.hadoop.fs.s3a.Constants.FS_S3A;

/**
 * S3A implementation of AbstractFileSystem.
 * This impl delegates to the S3AFileSystem
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class S3A extends DelegateToFileSystem {

  public S3A(URI theUri, Configuration conf)
      throws IOException, URISyntaxException {
    super(theUri, new S3AFileSystem(), conf,
        theUri.getScheme().isEmpty() ? FS_S3A : theUri.getScheme(), false);
  }

  @Override
  public int getUriDefaultPort() {
    // return Constants.S3A_DEFAULT_PORT;
    return super.getUriDefaultPort();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("S3A{");
    sb.append("URI =").append(fsImpl.getUri());
    sb.append("; fsImpl=").append(fsImpl);
    sb.append('}');
    return sb.toString();
  }

  /**
   * Close the file system; the FileContext API doesn't have an explicit close.
   */
  @Override
  protected void finalize() throws Throwable {
    fsImpl.close();
    super.finalize();
  }
}
