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

package org.apache.hadoop.security.ssl;

import java.io.IOException;
import java.util.ArrayList;

import javax.net.ssl.SSLEngine;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.mortbay.jetty.security.SslSelectChannelConnector;

/**
 * This subclass of the Jetty SslSelectChannelConnector exists solely to
 * control the TLS protocol versions allowed.  This is fallout from the
 * POODLE vulnerability (CVE-2014-3566), which requires that SSLv3 be disabled.
 * Only TLS 1.0 and later protocols are allowed.
 */
@InterfaceAudience.Private
public class SslSelectChannelConnectorSecure extends SslSelectChannelConnector {
  public static final Log LOG =
      LogFactory.getLog(SslSelectChannelConnectorSecure.class);

  public SslSelectChannelConnectorSecure() {
    super();
  }

  /**
   * Disable SSLv3 protocol.
   */
  @Override
  protected SSLEngine createSSLEngine() throws IOException {
    SSLEngine engine = super.createSSLEngine();
    ArrayList<String> nonSSLProtocols = new ArrayList<String>();
    for (String p : engine.getEnabledProtocols()) {
      if (!p.contains("SSLv3")) {
        nonSSLProtocols.add(p);
      }
    }
    engine.setEnabledProtocols(nonSSLProtocols.toArray(
        new String[nonSSLProtocols.size()]));
    return engine;
  }

  /* Override the broken isRunning() method (JETTY-1316). This bug is present
   * in 6.1.26. For the versions wihout this bug, it adds insignificant
   * overhead.
   */
  @Override
  public boolean isRunning() {
    if (super.isRunning()) {
      return true;
    }
    // We might be hitting JETTY-1316. If the internal state changed from
    // STARTING to STARTED in the middle of the check, the above call may
    // return false.  Check it one more time.
    LOG.warn("HttpServer Acceptor: isRunning is false. Rechecking.");
    try {
      Thread.sleep(10);
    } catch (InterruptedException ie) {
      // Mark this thread as interrupted. Someone up in the call chain
      // might care.
      Thread.currentThread().interrupt();
    }
    boolean runState = super.isRunning();
    LOG.warn("HttpServer Acceptor: isRunning is " + runState);
    return runState;
  }
}
