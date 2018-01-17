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
package org.apache.hadoop.fs.sftp;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * The SFtpFs implementation of AbstractFileSystem.
 * This impl delegates to the old FileSystem
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving /*Evolving for a release,to be changed to Stable */
public class SFtpFs extends DelegateToFileSystem {
  /**
   * This constructor has the signature needed by
   * {@link AbstractFileSystem#createFileSystem(URI, Configuration)}.
   *
   * @param theUri which must be that of localFs
   * @param conf
   * @throws IOException
   * @throws URISyntaxException
   */
  SFtpFs(final URI theUri, final Configuration conf) throws IOException,
      URISyntaxException {
    super(theUri, new SFTPFileSystem(), conf, FsConstants.SFTP_SCHEME, true);
  }
  
  @Override
  public int getUriDefaultPort() {
    return 22;
  }
  
  @Override
  @Deprecated
  public FsServerDefaults getServerDefaults() throws IOException {
    return SFtpConfigKeys.getServerDefaults();
  }

  @Override
  public FsServerDefaults getServerDefaults(final Path f) throws IOException {
    return SFtpConfigKeys.getServerDefaults();
  }
}
