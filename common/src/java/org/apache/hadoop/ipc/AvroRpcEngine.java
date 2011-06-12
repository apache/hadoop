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

package org.apache.hadoop.ipc;

import java.io.*;
import java.util.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import javax.net.SocketFactory;
import javax.security.auth.login.LoginException;

import org.apache.commons.logging.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.net.NetUtils;

import org.apache.avro.*;
import org.apache.avro.ipc.*;
import org.apache.avro.reflect.*;

/** Tunnel Avro-format RPC requests over a Hadoop {@link RPC} connection.  This
 * does not give cross-language wire compatibility, since the Hadoop RPC wire
 * format is non-standard, but it does permit use of Avro's protocol versioning
 * features for inter-Java RPCs. */
class AvroRpcEngine implements RpcEngine {
  private static final Log LOG = LogFactory.getLog(RPC.class);

  private static int VERSION = 0;

  // the implementation we tunnel through
  private static final RpcEngine ENGINE = new WritableRpcEngine();

  /** Tunnel an Avro RPC request and response through Hadoop's RPC. */
  private static interface TunnelProtocol extends VersionedProtocol {
    /** All Avro methods and responses go through this. */
    BufferListWritable call(BufferListWritable request) throws IOException;
  }

  /** A Writable that holds a List<ByteBuffer>, The Avro RPC Transceiver's
   * basic unit of data transfer.*/
  private static class BufferListWritable implements Writable {
    private List<ByteBuffer> buffers;

    public BufferListWritable() {}                // required for RPC Writables

    public BufferListWritable(List<ByteBuffer> buffers) {
      this.buffers = buffers;
    }

    public void readFields(DataInput in) throws IOException {
      int size = in.readInt();
      buffers = new ArrayList<ByteBuffer>(size);
      for (int i = 0; i < size; i++) {
        int length = in.readInt();
        ByteBuffer buffer = ByteBuffer.allocate(length);
        in.readFully(buffer.array(), 0, length);
        buffers.add(buffer);
      }
    }
  
    public void write(DataOutput out) throws IOException {
      out.writeInt(buffers.size());
      for (ByteBuffer buffer : buffers) {
        out.writeInt(buffer.remaining());
        out.write(buffer.array(), buffer.position(), buffer.remaining());
      }
    }
  }

  /** An Avro RPC Transceiver that tunnels client requests through Hadoop
   * RPC. */
  private static class ClientTransceiver extends Transceiver {
    private TunnelProtocol tunnel;
    private InetSocketAddress remote;
  
    public ClientTransceiver(InetSocketAddress addr,
                             UserGroupInformation ticket,
                             Configuration conf, SocketFactory factory)
      throws IOException {
      this.tunnel =
        (TunnelProtocol)ENGINE.getProxy(TunnelProtocol.class, VERSION,
                                        addr, ticket, conf, factory);
      this.remote = addr;
    }

    public String getRemoteName() { return remote.toString(); }

    public List<ByteBuffer> transceive(List<ByteBuffer> request)
      throws IOException {
      return tunnel.call(new BufferListWritable(request)).buffers;
    }

    public List<ByteBuffer> readBuffers() throws IOException {
      throw new UnsupportedOperationException();
    }

    public void writeBuffers(List<ByteBuffer> buffers) throws IOException {
      throw new UnsupportedOperationException();
    }

    public void close() throws IOException {
      ENGINE.stopProxy(tunnel);
    }
  }

  /** Construct a client-side proxy object that implements the named protocol,
   * talking to a server at the named address. */
  public Object getProxy(Class protocol, long clientVersion,
                         InetSocketAddress addr, UserGroupInformation ticket,
                         Configuration conf, SocketFactory factory)
    throws IOException {
    return Proxy.newProxyInstance
      (protocol.getClassLoader(),
       new Class[] { protocol },
       new Invoker(protocol, addr, ticket, conf, factory));
  }

  /** Stop this proxy. */
  public void stopProxy(Object proxy) {
    try {
      ((Invoker)Proxy.getInvocationHandler(proxy)).close();
    } catch (IOException e) {
      LOG.warn("Error while stopping "+proxy, e);
    }
  }

  private static class Invoker implements InvocationHandler, Closeable {
    private final ClientTransceiver tx;
    private final ReflectRequestor requestor;
    public Invoker(Class<?> protocol, InetSocketAddress addr,
                   UserGroupInformation ticket, Configuration conf,
                   SocketFactory factory) throws IOException {
      this.tx = new ClientTransceiver(addr, ticket, conf, factory);
      this.requestor = new ReflectRequestor(protocol, tx);
    }
    @Override public Object invoke(Object proxy, Method method, Object[] args) 
      throws Throwable {
      return requestor.invoke(proxy, method, args);
    }
    public void close() throws IOException {
      tx.close();
    }
  }

  /** An Avro RPC Responder that can process requests passed via Hadoop RPC. */
  private static class TunnelResponder extends ReflectResponder
    implements TunnelProtocol {

    public TunnelResponder(Class iface, Object impl) {
      super(iface, impl);
    }

    public long getProtocolVersion(String protocol, long version)
      throws IOException {
      return VERSION;
    }

    public BufferListWritable call(final BufferListWritable request)
      throws IOException {
      return new BufferListWritable(respond(request.buffers));
    }
  }

  public Object[] call(Method method, Object[][] params,
                       InetSocketAddress[] addrs, UserGroupInformation ticket,
                       Configuration conf) throws IOException {
    throw new UnsupportedOperationException();
  }

  /** Construct a server for a protocol implementation instance listening on a
   * port and address. */
  public RPC.Server getServer(Class iface, Object impl, String bindAddress,
                              int port, int numHandlers, boolean verbose,
                              Configuration conf, 
                       SecretManager<? extends TokenIdentifier> secretManager
                              ) throws IOException {
    return ENGINE.getServer(TunnelProtocol.class,
                            new TunnelResponder(iface, impl),
                            bindAddress, port, numHandlers, verbose, conf, 
                            secretManager);
  }

}
