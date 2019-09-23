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
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.util.StringUtils;

import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;

/**
 * CLI entrypoint of the CSI service daemon.
 */
@Command(name = "ozone csi",
    hidden = true, description = "CSI service daemon.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true)
public class CsiServer extends GenericCli implements Callable<Void> {

  private final static Logger LOG = LoggerFactory.getLogger(CsiServer.class);

  @Override
  public Void call() throws Exception {
    OzoneConfiguration ozoneConfiguration = createOzoneConfiguration();
    CsiConfig csiConfig = ozoneConfiguration.getObject(CsiConfig.class);

    OzoneClient rpcClient = OzoneClientFactory.getRpcClient(ozoneConfiguration);

    EpollEventLoopGroup group = new EpollEventLoopGroup();

    if (csiConfig.getVolumeOwner().isEmpty()) {
      throw new IllegalArgumentException(
          "ozone.csi.owner is not set. You should set this configuration "
              + "variable to define which user should own all the created "
              + "buckets.");
    }

    Server server =
        NettyServerBuilder
            .forAddress(new DomainSocketAddress(csiConfig.getSocketPath()))
            .channelType(EpollServerDomainSocketChannel.class)
            .workerEventLoopGroup(group)
            .bossEventLoopGroup(group)
            .addService(new IdentitiyService())
            .addService(new ControllerService(rpcClient,
                csiConfig.getDefaultVolumeSize(), csiConfig.getVolumeOwner()))
            .addService(new NodeService(csiConfig))
            .build();

    server.start();
    server.awaitTermination();
    rpcClient.close();
    return null;
  }

  public static void main(String[] args) {

    StringUtils.startupShutdownMessage(CsiServer.class, args, LOG);
    new CsiServer().run(args);
  }

  /**
   * Configuration settings specific to the CSI server.
   */
  @ConfigGroup(prefix = "ozone.csi")
  public static class CsiConfig {
    private String socketPath;
    private long defaultVolumeSize;
    private String s3gAddress;
    private String volumeOwner;

    public String getSocketPath() {
      return socketPath;
    }

    public String getVolumeOwner() {
      return volumeOwner;
    }

    @Config(key = "owner",
        defaultValue = "",
        description =
            "This is the username which is used to create the requested "
                + "storage. Used as a hadoop username and the generated ozone"
                + " volume used to store all the buckets. WARNING: It can "
                + "be a security hole to use CSI in a secure environments as "
                + "ALL the users can request the mount of a specific bucket "
                + "via the CSI interface.",
        tags = ConfigTag.STORAGE)
    public void setVolumeOwner(String volumeOwner) {
      this.volumeOwner = volumeOwner;
    }

    @Config(key = "socket",
        defaultValue = "/var/lib/csi.sock",
        description =
            "The socket where all the CSI services will listen (file name).",
        tags = ConfigTag.STORAGE)
    public void setSocketPath(String socketPath) {
      this.socketPath = socketPath;
    }

    public long getDefaultVolumeSize() {
      return defaultVolumeSize;
    }

    @Config(key = "default-volume-size",
        defaultValue = "1000000000",
        description =
            "The default size of the create volumes (if not specified).",
        tags = ConfigTag.STORAGE)
    public void setDefaultVolumeSize(long defaultVolumeSize) {
      this.defaultVolumeSize = defaultVolumeSize;
    }

    public String getS3gAddress() {
      return s3gAddress;
    }

    @Config(key = "s3g.address",
        defaultValue = "http://localhost:9878",
        description =
            "The default size of the created volumes (if not specified in the"
                + " requests).",
        tags = ConfigTag.STORAGE)
    public void setS3gAddress(String s3gAddress) {
      this.s3gAddress = s3gAddress;
    }
  }
}
