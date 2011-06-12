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

package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapred.LocalJobRunner;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Class to instantiate ClientProtocol proxy handle.
 *
 */
public abstract class ClientFactory {

  @SuppressWarnings("unchecked")
  public static ClientProtocol create(Configuration conf) throws IOException {
    Class<ClientFactory> factory = (Class<ClientFactory>) conf.getClass(
        "mapreduce.clientfactory.class.name", 
        DefaultClientFactory.class); 
    try {
      return factory.newInstance().createClient(conf);
    } catch (Exception e) {
      throw new IOException("could not create ClientProtocol", e);
    }
  }

  protected abstract ClientProtocol createClient(Configuration conf) 
        throws IOException;

  //the default factory handles the backward compatibility
  public static class DefaultClientFactory extends ClientFactory {

    @Override
    protected ClientProtocol createClient(Configuration conf)
        throws IOException {
      String tracker = conf.get("mapreduce.jobtracker.address");
      if ("local".equals(tracker)) {
        return createLocalClient(conf);
      } else {
        return createJTClient(conf);
      }
    }
  }

  public ClientProtocol createLocalClient(Configuration conf) 
        throws IOException {
    conf.setInt("mapreduce.job.maps", 1);
    return new LocalJobRunner(conf);
  }
  
  public ClientProtocol createJTClient(Configuration conf) throws IOException {
    return createJTClient(JobTracker.getAddress(conf), conf);
  }

  public ClientProtocol createJTClient(InetSocketAddress addr,
        Configuration conf) throws IOException {
    return (ClientProtocol) RPC.getProxy(ClientProtocol.class,
        ClientProtocol.versionID, addr, 
          UserGroupInformation.getCurrentUser(), conf,
          NetUtils.getSocketFactory(conf, ClientProtocol.class));
    }
}
