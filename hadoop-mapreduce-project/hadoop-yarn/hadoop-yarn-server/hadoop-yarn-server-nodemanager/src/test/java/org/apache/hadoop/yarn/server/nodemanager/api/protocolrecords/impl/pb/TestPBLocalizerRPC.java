package org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.impl.pb;

import java.net.InetSocketAddress;

import org.apache.avro.ipc.Server;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.nodemanager.api.LocalizationProtocol;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerAction;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerHeartbeatResponse;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerStatus;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestPBLocalizerRPC {

  static final RecordFactory recordFactory = createPBRecordFactory();

  static RecordFactory createPBRecordFactory() {
    Configuration conf = new Configuration();
    conf.set(RecordFactoryProvider.RPC_SERIALIZER_KEY, "protocolbuffers");
    return RecordFactoryProvider.getRecordFactory(conf);
  }

  static class LocalizerService implements LocalizationProtocol {
    private final InetSocketAddress locAddr;
    private Server server;
    LocalizerService(InetSocketAddress locAddr) {
      this.locAddr = locAddr;
    }

    public void start() {
      Configuration conf = new Configuration();
      YarnRPC rpc = YarnRPC.create(conf);
      server = rpc.getServer(
          LocalizationProtocol.class, this, locAddr, conf, null, 1);
      server.start();
    }

    public void stop() {
      if (server != null) {
        server.close();
      }
    }

    @Override
    public LocalizerHeartbeatResponse heartbeat(LocalizerStatus status) {
      return dieHBResponse();
    }
  }

  static LocalizerHeartbeatResponse dieHBResponse() {
    LocalizerHeartbeatResponse response =
      recordFactory.newRecordInstance(LocalizerHeartbeatResponse.class);
    response.setLocalizerAction(LocalizerAction.DIE);
    return response;
  }

  @Test
  public void testLocalizerRPC() throws Exception {
    InetSocketAddress locAddr = new InetSocketAddress("0.0.0.0", 4344);
    LocalizerService server = new LocalizerService(locAddr);
    try {
      server.start();
      Configuration conf = new Configuration();
      YarnRPC rpc = YarnRPC.create(conf);
      LocalizationProtocol client = (LocalizationProtocol)
        rpc.getProxy(LocalizationProtocol.class, locAddr, conf);
      LocalizerStatus status =
        recordFactory.newRecordInstance(LocalizerStatus.class);
      status.setLocalizerId("localizer0");
      LocalizerHeartbeatResponse response = client.heartbeat(status);
      assertEquals(dieHBResponse(), response);
    } finally {
      server.stop();
    }
    assertTrue(true);
  }

}
