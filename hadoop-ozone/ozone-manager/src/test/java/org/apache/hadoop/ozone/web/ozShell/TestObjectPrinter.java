/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.web.ozShell;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;

import org.junit.Assert;
import static org.junit.Assert.*;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test the json object printer.
 */
public class TestObjectPrinter {

  @Test
  public void printObjectAsJson() throws IOException {

    OzoneConfiguration conf = new OzoneConfiguration();
    OzoneVolume volume =
        new OzoneVolume(conf, Mockito.mock(ClientProtocol.class), "name",
            "admin", "owner", 1L, 0L,
            new ArrayList<>());

    String result = ObjectPrinter.getObjectAsJson(volume);
    Assert.assertTrue("Result is not a proper json",
        result.contains("\"owner\""));
  }
}