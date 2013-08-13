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
package org.apache.directory.server.kerberos.shared.keytab;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

//This is a hack for ApacheDS 2.0.0-M14 to be able to create
//keytab files with more than one principal.
//It needs to be in this package because the KeytabEncoder class is package 
// private.
//This class can be removed once jira DIRSERVER-1882
// (https://issues.apache.org/jira/browse/DIRSERVER-1882) solved
public class HackedKeytab extends Keytab {

  private byte[] keytabVersion = VERSION_52;

  public void write( File file, int principalCount ) throws IOException
  {
    HackedKeytabEncoder writer = new HackedKeytabEncoder();
    ByteBuffer buffer = writer.write( keytabVersion, getEntries(),
            principalCount );
    writeFile( buffer, file );
  }

}