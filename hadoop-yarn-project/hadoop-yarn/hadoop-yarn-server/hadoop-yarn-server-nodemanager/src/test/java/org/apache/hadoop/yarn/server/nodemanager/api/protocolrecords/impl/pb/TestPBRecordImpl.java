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
package org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.impl.pb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;

import org.apache.hadoop.yarn.api.records.URL;
import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.SerializedException;
import org.apache.hadoop.yarn.api.records.impl.pb.LocalResourcePBImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerServiceProtos.LocalResourceStatusProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerServiceProtos.LocalizerHeartbeatResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerServiceProtos.LocalizerStatusProto;
import org.apache.hadoop.yarn.server.nodemanager.api.ResourceLocalizationSpec;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalResourceStatus;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerAction;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerHeartbeatResponse;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerStatus;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.ResourceStatusType;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.junit.Test;

public class TestPBRecordImpl {

  static final RecordFactory recordFactory = createPBRecordFactory();

  static RecordFactory createPBRecordFactory() {
    Configuration conf = new Configuration();
    return RecordFactoryProvider.getRecordFactory(conf);
  }

  static LocalResource createResource() {
    LocalResource ret = recordFactory.newRecordInstance(LocalResource.class);
    assertTrue(ret instanceof LocalResourcePBImpl);
    ret.setResource(URL.fromPath(new Path(
      "hdfs://y.ak:8020/foo/bar")));
    ret.setSize(4344L);
    ret.setTimestamp(3141592653589793L);
    ret.setVisibility(LocalResourceVisibility.PUBLIC);
    return ret;
  }

  static LocalResourceStatus createLocalResourceStatus() {
    LocalResourceStatus ret =
      recordFactory.newRecordInstance(LocalResourceStatus.class);
    assertTrue(ret instanceof LocalResourceStatusPBImpl);
    ret.setResource(createResource());
    ret.setLocalPath(
        URL.fromPath(
          new Path("file:///local/foo/bar")));
    ret.setStatus(ResourceStatusType.FETCH_SUCCESS);
    ret.setLocalSize(4443L);
    Exception e = new Exception("Dingos.");
    e.setStackTrace(new StackTraceElement[] {
        new StackTraceElement("foo", "bar", "baz", 10),
        new StackTraceElement("sbb", "one", "onm", 10) });
    ret.setException(SerializedException.newInstance(e));
    return ret;
  }

  static LocalizerStatus createLocalizerStatus() {
    LocalizerStatus ret =
      recordFactory.newRecordInstance(LocalizerStatus.class);
    assertTrue(ret instanceof LocalizerStatusPBImpl);
    ret.setLocalizerId("localizer0");
    ret.addResourceStatus(createLocalResourceStatus());
    return ret;
  }

  static LocalizerHeartbeatResponse createLocalizerHeartbeatResponse() 
      throws URISyntaxException {
    LocalizerHeartbeatResponse ret =
      recordFactory.newRecordInstance(LocalizerHeartbeatResponse.class);
    assertTrue(ret instanceof LocalizerHeartbeatResponsePBImpl);
    ret.setLocalizerAction(LocalizerAction.LIVE);
    LocalResource rsrc = createResource();
    ArrayList<ResourceLocalizationSpec> rsrcs =
      new ArrayList<ResourceLocalizationSpec>();
    ResourceLocalizationSpec resource =
      recordFactory.newRecordInstance(ResourceLocalizationSpec.class);
    resource.setResource(rsrc);
    resource.setDestinationDirectory(
        URL.fromPath((new Path("/tmp" + System.currentTimeMillis()))));
    rsrcs.add(resource);
    ret.setResourceSpecs(rsrcs);
    System.out.println(resource);
    return ret;
  }

  @Test(timeout=10000)
  public void testLocalResourceStatusSerDe() throws Exception {
    LocalResourceStatus rsrcS = createLocalResourceStatus();
    assertTrue(rsrcS instanceof LocalResourceStatusPBImpl);
    LocalResourceStatusPBImpl rsrcPb = (LocalResourceStatusPBImpl) rsrcS;
    DataOutputBuffer out = new DataOutputBuffer();
    rsrcPb.getProto().writeDelimitedTo(out);
    DataInputBuffer in = new DataInputBuffer();
    in.reset(out.getData(), 0, out.getLength());
    LocalResourceStatusProto rsrcPbD =
      LocalResourceStatusProto.parseDelimitedFrom(in);
    assertNotNull(rsrcPbD);
    LocalResourceStatus rsrcD =
      new LocalResourceStatusPBImpl(rsrcPbD);

    assertEquals(rsrcS, rsrcD);
    assertEquals(createResource(), rsrcS.getResource());
    assertEquals(createResource(), rsrcD.getResource());
  }

  @Test(timeout=10000)
  public void testLocalizerStatusSerDe() throws Exception {
    LocalizerStatus rsrcS = createLocalizerStatus();
    assertTrue(rsrcS instanceof LocalizerStatusPBImpl);
    LocalizerStatusPBImpl rsrcPb = (LocalizerStatusPBImpl) rsrcS;
    DataOutputBuffer out = new DataOutputBuffer();
    rsrcPb.getProto().writeDelimitedTo(out);
    DataInputBuffer in = new DataInputBuffer();
    in.reset(out.getData(), 0, out.getLength());
    LocalizerStatusProto rsrcPbD =
      LocalizerStatusProto.parseDelimitedFrom(in);
    assertNotNull(rsrcPbD);
    LocalizerStatus rsrcD =
      new LocalizerStatusPBImpl(rsrcPbD);

    assertEquals(rsrcS, rsrcD);
    assertEquals("localizer0", rsrcS.getLocalizerId());
    assertEquals("localizer0", rsrcD.getLocalizerId());
    assertEquals(createLocalResourceStatus(), rsrcS.getResourceStatus(0));
    assertEquals(createLocalResourceStatus(), rsrcD.getResourceStatus(0));
  }

  @Test(timeout=10000)
  public void testLocalizerHeartbeatResponseSerDe() throws Exception {
    LocalizerHeartbeatResponse rsrcS = createLocalizerHeartbeatResponse();
    assertTrue(rsrcS instanceof LocalizerHeartbeatResponsePBImpl);
    LocalizerHeartbeatResponsePBImpl rsrcPb =
      (LocalizerHeartbeatResponsePBImpl) rsrcS;
    DataOutputBuffer out = new DataOutputBuffer();
    rsrcPb.getProto().writeDelimitedTo(out);
    DataInputBuffer in = new DataInputBuffer();
    in.reset(out.getData(), 0, out.getLength());
    LocalizerHeartbeatResponseProto rsrcPbD =
      LocalizerHeartbeatResponseProto.parseDelimitedFrom(in);
    assertNotNull(rsrcPbD);
    LocalizerHeartbeatResponse rsrcD =
      new LocalizerHeartbeatResponsePBImpl(rsrcPbD);

    assertEquals(rsrcS, rsrcD);
    assertEquals(createResource(), rsrcS.getResourceSpecs().get(0).getResource());
    assertEquals(createResource(), rsrcD.getResourceSpecs().get(0).getResource());
  }


  @Test(timeout=10000)
  public void testSerializedExceptionDeSer() throws Exception{
    // without cause
    YarnException yarnEx = new YarnException("Yarn_Exception");
    SerializedException serEx = SerializedException.newInstance(yarnEx);
    Throwable throwable = serEx.deSerialize();
    Assert.assertEquals(yarnEx.getClass(), throwable.getClass());
    Assert.assertEquals(yarnEx.getMessage(), throwable.getMessage());

    // with cause
    IOException ioe = new IOException("Test_IOException");
    RuntimeException runtimeException =
        new RuntimeException("Test_RuntimeException", ioe);
    YarnException yarnEx2 =
        new YarnException("Test_YarnException", runtimeException);

    SerializedException serEx2 = SerializedException.newInstance(yarnEx2);
    Throwable throwable2 = serEx2.deSerialize();
    throwable2.printStackTrace();
    Assert.assertEquals(yarnEx2.getClass(), throwable2.getClass());
    Assert.assertEquals(yarnEx2.getMessage(), throwable2.getMessage());

    Assert.assertEquals(runtimeException.getClass(), throwable2.getCause().getClass());
    Assert.assertEquals(runtimeException.getMessage(), throwable2.getCause().getMessage());

    Assert.assertEquals(ioe.getClass(), throwable2.getCause().getCause().getClass());
    Assert.assertEquals(ioe.getMessage(), throwable2.getCause().getCause().getMessage());
  }
}
