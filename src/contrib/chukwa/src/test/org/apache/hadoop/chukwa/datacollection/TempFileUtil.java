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

package org.apache.hadoop.chukwa.datacollection;

import java.io.*;
import java.util.Random;

public class TempFileUtil {
  public static File makeBinary(int length) throws IOException {
    File tmpOutput = new File(System.getProperty("test.build.data", "/tmp"),"chukwaTest");
    FileOutputStream fos = new FileOutputStream(tmpOutput);
    Random r = new Random();
    byte[] randomData = new byte[ length];
    r.nextBytes(randomData);
    randomData[ length-1] = '\n';//need data to end with \n since default tailer uses that
    fos.write(randomData);
    fos.flush();
    fos.close();
    return tmpOutput;
  }
}
