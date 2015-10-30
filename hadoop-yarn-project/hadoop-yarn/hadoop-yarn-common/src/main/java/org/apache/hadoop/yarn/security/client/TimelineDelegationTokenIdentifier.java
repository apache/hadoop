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

package org.apache.hadoop.yarn.security.client;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenRenewer;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.exceptions.YarnException;

@Public
@Evolving
public class TimelineDelegationTokenIdentifier extends YARNDelegationTokenIdentifier {

  public static final Text KIND_NAME = new Text("TIMELINE_DELEGATION_TOKEN");

  public TimelineDelegationTokenIdentifier() {

  }

  /**
   * Create a new timeline delegation token identifier
   *
   * @param owner the effective username of the token owner
   * @param renewer the username of the renewer
   * @param realUser the real username of the token owner
   */
  public TimelineDelegationTokenIdentifier(Text owner, Text renewer,
      Text realUser) {
    super(owner, renewer, realUser);
  }

  @Override
  public Text getKind() {
    return KIND_NAME;
  }

  @InterfaceAudience.Private
  public static class Renewer extends TokenRenewer {

    @Override
    public boolean handleKind(Text kind) {
      return KIND_NAME.equals(kind);
    }

    @Override
    public boolean isManaged(Token<?> token) throws IOException {
      return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    public long renew(Token<?> token, Configuration conf) throws IOException,
        InterruptedException {
      TimelineClient client = TimelineClient.createTimelineClient();
      try {
        client.init(conf);
        client.start();
        return client.renewDelegationToken(
            (Token<TimelineDelegationTokenIdentifier>) token);
      } catch (YarnException e) {
        throw new IOException(e);
      } finally {
        client.stop();
      }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void cancel(Token<?> token, Configuration conf) throws IOException,
        InterruptedException {
      TimelineClient client = TimelineClient.createTimelineClient();
      try {
        client.init(conf);
        client.start();
        client.cancelDelegationToken(
            (Token<TimelineDelegationTokenIdentifier>) token);
      } catch (YarnException e) {
        throw new IOException(e);
      } finally {
        client.stop();
      }
    }
  }

}
