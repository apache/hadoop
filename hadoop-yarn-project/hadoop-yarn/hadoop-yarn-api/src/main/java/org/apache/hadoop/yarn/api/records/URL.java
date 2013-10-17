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

package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p><code>URL</code> represents a serializable {@link java.net.URL}.</p>
 */
@Public
@Stable
public abstract class URL {

  @Public
  @Stable
  public static URL newInstance(String scheme, String host, int port, String file) {
    URL url = Records.newRecord(URL.class);
    url.setScheme(scheme);
    url.setHost(host);
    url.setPort(port);
    url.setFile(file);
    return url;
  }

  /**
   * Get the scheme of the URL.
   * @return scheme of the URL
   */
  @Public
  @Stable
  public abstract String getScheme();
  
  /**
   * Set the scheme of the URL
   * @param scheme scheme of the URL
   */
  @Public
  @Stable
  public abstract void setScheme(String scheme);

  /**
   * Get the user info of the URL.
   * @return user info of the URL
   */
  @Public
  @Stable
  public abstract String getUserInfo();
  
  /**
   * Set the user info of the URL.
   * @param userInfo user info of the URL
   */
  @Public
  @Stable
  public abstract void setUserInfo(String userInfo);

  /**
   * Get the host of the URL.
   * @return host of the URL
   */
  @Public
  @Stable
  public abstract String getHost();
  
  /**
   * Set the host of the URL.
   * @param host host of the URL
   */
  @Public
  @Stable
  public abstract void setHost(String host);

  /**
   * Get the port of the URL.
   * @return port of the URL
   */
  @Public
  @Stable
  public abstract int getPort();
  
  /**
   * Set the port of the URL
   * @param port port of the URL
   */
  @Public
  @Stable
  public abstract void setPort(int port);

  /**
   * Get the file of the URL.
   * @return file of the URL
   */
  @Public
  @Stable
  public abstract String getFile();
  
  /**
   * Set the file of the URL.
   * @param file file of the URL
   */
  @Public
  @Stable
  public abstract void setFile(String file);
}
