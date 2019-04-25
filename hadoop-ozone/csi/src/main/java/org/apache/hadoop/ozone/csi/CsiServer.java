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
package org.apache.hadoop.ozone.csi;

import java.util.concurrent.Callable;

import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;

import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import picocli.CommandLine.Command;

/**
 * CLI entrypoint of the CSI service daemon.
 */
@Command(name = "ozone csi",
    hidden = true, description = "CSI service daemon.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true)
public class CsiServer extends GenericCli implements Callable<Void> {

  @Override
  public Void call() throws Exception {
    OzoneConfiguration ozoneConfiguration = createOzoneConfiguration();
    String unixSocket = ozoneConfiguration
        .get(CsiConfigurationValues.OZONE_CSI_SOCKET,
            CsiConfigurationValues.OZONE_CSI_SOCKET_DEFAULT);
    OzoneClient rpcClient = OzoneClientFactory.getRpcClient(ozoneConfiguration);

    EpollEventLoopGroup group = new EpollEventLoopGroup();

    long defaultVolumeSize = ozoneConfiguration
        .getLongBytes(CsiConfigurationValues.OZONE_CSI_DEFAULT_VOLUME_SIZE,
            CsiConfigurationValues.OZONE_CSI_DEFAULT_VOLUME_SIZE_DEFAULT);
    Server server =
        NettyServerBuilder.forAddress(new DomainSocketAddress(unixSocket))
            .channelType(EpollServerDomainSocketChannel.class)
            .workerEventLoopGroup(group)
            .bossEventLoopGroup(group)
            .addService(new IdentitiyService())
            .addService(new ControllerService(rpcClient, defaultVolumeSize))
            .addService(new NodeService(ozoneConfiguration))
            .build();

    server.start();
    server.awaitTermination();
    rpcClient.close();
    return null;
  }

  public static void main(String[] args) {
    new CsiServer().run(args);
  }
}
