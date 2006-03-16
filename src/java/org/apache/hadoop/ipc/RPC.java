/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.lang.reflect.Proxy;
import java.lang.reflect.Method;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;

import java.net.InetSocketAddress;
import java.util.logging.Logger;
import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.LogFormatter;

/** A simple RPC mechanism.
 *
 * A <i>protocol</i> is a Java interface.  All parameters and return types must
 * be one of:
 *
 * <ul> <li>a primitive type, <code>boolean</code>, <code>byte</code>,
 * <code>char</code>, <code>short</code>, <code>int</code>, <code>long</code>,
 * <code>float</code>, <code>double</code>, or <code>void</code>; or</li>
 *
 * <li>a {@link String}; or</li>
 *
 * <li>a {@link Writable}; or</li>
 *
 * <li>an array of the above types</li> </ul>
 *
 * All methods in the protocol should throw only IOException.  No field data of
 * the protocol instance is transmitted.
 */
public class RPC {
  private static final Logger LOG =
    LogFormatter.getLogger("org.apache.hadoop.ipc.RPC");

  private RPC() {}                                  // no public ctor


  /** A method invocation, including the method name and its parameters.*/
  private static class Invocation implements Writable, Configurable {
    private String methodName;
    private Class[] parameterClasses;
    private Object[] parameters;
    private Configuration conf;

    public Invocation() {}

    public Invocation(Method method, Object[] parameters) {
      this.methodName = method.getName();
      this.parameterClasses = method.getParameterTypes();
      this.parameters = parameters;
    }

    /** The name of the method invoked. */
    public String getMethodName() { return methodName; }

    /** The parameter classes. */
    public Class[] getParameterClasses() { return parameterClasses; }

    /** The parameter instances. */
    public Object[] getParameters() { return parameters; }

    public void readFields(DataInput in) throws IOException {
      methodName = UTF8.readString(in);
      parameters = new Object[in.readInt()];
      parameterClasses = new Class[parameters.length];
      ObjectWritable objectWritable = new ObjectWritable();
      for (int i = 0; i < parameters.length; i++) {
        parameters[i] = ObjectWritable.readObject(in, objectWritable, this.conf);
        parameterClasses[i] = objectWritable.getDeclaredClass();
      }
    }

    public void write(DataOutput out) throws IOException {
      UTF8.writeString(out, methodName);
      out.writeInt(parameterClasses.length);
      for (int i = 0; i < parameterClasses.length; i++) {
        ObjectWritable.writeObject(out, parameters[i], parameterClasses[i]);
      }
    }

    public String toString() {
      StringBuffer buffer = new StringBuffer();
      buffer.append(methodName);
      buffer.append("(");
      for (int i = 0; i < parameters.length; i++) {
        if (i != 0)
          buffer.append(", ");
        buffer.append(parameters[i]);
      }
      buffer.append(")");
      return buffer.toString();
    }

    public void setConf(Configuration conf) {
      this.conf = conf;
    }

    public Configuration getConf() {
      return this.conf;
    }

  }

  //TODO mb@media-style.com: static client or non-static client?
  private static Client CLIENT;

  private static class Invoker implements InvocationHandler {
    private InetSocketAddress address;

    public Invoker(InetSocketAddress address, Configuration conf) {
      this.address = address;
      CLIENT = (Client) conf.getObject(Client.class.getName());
      if(CLIENT == null) {
          CLIENT = new Client(ObjectWritable.class, conf);
          conf.setObject(Client.class.getName(), CLIENT);
      }
    }

    public Object invoke(Object proxy, Method method, Object[] args)
      throws Throwable {
      ObjectWritable value = (ObjectWritable)
        CLIENT.call(new Invocation(method, args), address);
      return value.get();
    }
  }

  /** Construct a client-side proxy object that implements the named protocol,
   * talking to a server at the named address. */
  public static Object getProxy(Class protocol, InetSocketAddress addr, Configuration conf) {
    return Proxy.newProxyInstance(protocol.getClassLoader(),
                                  new Class[] { protocol },
                                  new Invoker(addr, conf));
  }

  /** Expert: Make multiple, parallel calls to a set of servers. */
  public static Object[] call(Method method, Object[][] params,
                              InetSocketAddress[] addrs, Configuration conf)
    throws IOException {

    Invocation[] invocations = new Invocation[params.length];
    for (int i = 0; i < params.length; i++)
      invocations[i] = new Invocation(method, params[i]);
    CLIENT = (Client) conf.getObject(Client.class.getName());
    if(CLIENT == null) {
        CLIENT = new Client(ObjectWritable.class, conf);
        conf.setObject(Client.class.getName(), CLIENT);
    }
    Writable[] wrappedValues = CLIENT.call(invocations, addrs);
    
    if (method.getReturnType() == Void.TYPE) {
      return null;
    }

    Object[] values =
      (Object[])Array.newInstance(method.getReturnType(),wrappedValues.length);
    for (int i = 0; i < values.length; i++)
      if (wrappedValues[i] != null)
        values[i] = ((ObjectWritable)wrappedValues[i]).get();
    
    return values;
  }
  

  /** Construct a server for a protocol implementation instance listening on a
   * port. */
  public static Server getServer(final Object instance, final int port, Configuration conf) {
    return getServer(instance, port, 1, false, conf);
  }

  /** Construct a server for a protocol implementation instance listening on a
   * port. */
  public static Server getServer(final Object instance, final int port,
                                 final int numHandlers,
                                 final boolean verbose, Configuration conf) {
    return new Server(instance, conf, port, numHandlers, verbose);
  }
        
  /** An RPC Server. */
  public static class Server extends org.apache.hadoop.ipc.Server {
    private Object instance;
    private Class implementation;
    private boolean verbose;

    /** Construct an RPC server.
     * @param instance the instance whose methods will be called
     * @param conf the configuration to use
     * @param port the port to listen for connections on
     */
    public Server(Object instance, Configuration conf, int port) {
      this(instance, conf, port, 1, false);
    }

    /** Construct an RPC server.
     * @param instance the instance whose methods will be called
     * @param conf the configuration to use
     * @param port the port to listen for connections on
     * @param numHandlers the number of method handler threads to run
     * @param verbose whether each call should be logged
     */
    public Server(Object instance, Configuration conf, int port,
                  int numHandlers, boolean verbose) {
      super(port, Invocation.class, numHandlers, conf);
      this.instance = instance;
      this.implementation = instance.getClass();
      this.verbose = verbose;
    }

    public Writable call(Writable param) throws IOException {
      try {
        Invocation call = (Invocation)param;
        if (verbose) log("Call: " + call);
        
        Method method =
          implementation.getMethod(call.getMethodName(),
                                   call.getParameterClasses());

        Object value = method.invoke(instance, call.getParameters());
        if (verbose) log("Return: "+value);

        return new ObjectWritable(method.getReturnType(), value);

      } catch (InvocationTargetException e) {
        Throwable target = e.getTargetException();
        if (target instanceof IOException) {
          throw (IOException)target;
        } else {
          IOException ioe = new IOException(target.toString());
          ioe.setStackTrace(target.getStackTrace());
          throw ioe;
        }
      } catch (Throwable e) {
        IOException ioe = new IOException(e.toString());
        ioe.setStackTrace(e.getStackTrace());
        throw ioe;
      }
    }
  }

  private static void log(String value) {
    if (value!= null && value.length() > 55)
      value = value.substring(0, 55)+"...";
    LOG.info(value);
  }
  
}
