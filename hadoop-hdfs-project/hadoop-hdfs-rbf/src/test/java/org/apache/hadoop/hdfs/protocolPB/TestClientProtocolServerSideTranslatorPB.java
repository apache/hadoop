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

package org.apache.hadoop.hdfs.protocolPB;

import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.ipc.TestRpcBase;
import org.apache.hadoop.ipc.protobuf.TestProtos;
import org.apache.hadoop.thirdparty.protobuf.RpcController;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestClientProtocolServerSideTranslatorPB extends TestRpcBase.PBServerImpl {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestClientProtocolServerSideTranslatorPB.class);
  private final int processTime;

  public TestClientProtocolServerSideTranslatorPB(int processTime) {
    this.processTime = processTime;
  }

  @Override
  public TestProtos.EmptyResponseProto error(
      RpcController unused, TestProtos.EmptyRequestProto request)
      throws ServiceException {
    long start = Time.monotonicNow();
    try {
      Thread.sleep(processTime);
      throw new ServiceException("error", new StandbyException("test!"));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      LOG.info("rpc server error cost: {}ms", Time.monotonicNow() - start);
    }
    return null;
  }

  @Override
  public TestProtos.EchoResponseProto echo(
      RpcController unused, TestProtos.EchoRequestProto request) throws ServiceException {
    TestProtos.EchoResponseProto res = null;
    long start = Time.monotonicNow();
    try {
      Thread.sleep(processTime);
      res = super.echo(unused, request);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      LOG.info("rpc server echo: {}, result: {}, cost: {}ms", request.getMessage(),
          res.getMessage(), Time.monotonicNow() - start);
    }
    return res;
  }

  @Override
  public TestProtos.AddResponseProto add(
      RpcController controller, TestProtos.AddRequestProto request) throws ServiceException {
    TestProtos.AddResponseProto res = null;
    long start = Time.monotonicNow();
    try {
      Thread.sleep(processTime);
      res = super.add(controller, request);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      LOG.info("rpc server add: {} {}, result: {}, cost: {}ms",
          request.getParam1(), request.getParam2(), res.getResult(), Time.monotonicNow() - start);
    }
    return res;
  }
}
