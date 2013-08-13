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

import org.apache.directory.shared.kerberos.components.EncryptionKey;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

//This is a hack for ApacheDS 2.0.0-M14 to be able to create
//keytab files with more than one principal.
//It needs to be in this package because the KeytabEncoder class is package 
// private.
//This class can be removed once jira DIRSERVER-1882
// (https://issues.apache.org/jira/browse/DIRSERVER-1882) solved
class HackedKeytabEncoder extends KeytabEncoder {

  ByteBuffer write( byte[] keytabVersion, List<KeytabEntry> entries,
                    int principalCount )
  {
    ByteBuffer buffer = ByteBuffer.allocate( 512 * principalCount);
    putKeytabVersion(buffer, keytabVersion);
    putKeytabEntries( buffer, entries );
    buffer.flip();
    return buffer;
  }

  private void putKeytabVersion( ByteBuffer buffer, byte[] version )
  {
    buffer.put( version );
  }

  private void putKeytabEntries( ByteBuffer buffer, List<KeytabEntry> entries )
  {
    Iterator<KeytabEntry> iterator = entries.iterator();

    while ( iterator.hasNext() )
    {
      ByteBuffer entryBuffer = putKeytabEntry( iterator.next() );
      int size = entryBuffer.position();

      entryBuffer.flip();

      buffer.putInt( size );
      buffer.put( entryBuffer );
    }
  }

  private ByteBuffer putKeytabEntry( KeytabEntry entry )
  {
    ByteBuffer buffer = ByteBuffer.allocate( 100 );

    putPrincipalName( buffer, entry.getPrincipalName() );

    buffer.putInt( ( int ) entry.getPrincipalType() );

    buffer.putInt( ( int ) ( entry.getTimeStamp().getTime() / 1000 ) );

    buffer.put( entry.getKeyVersion() );

    putKeyBlock( buffer, entry.getKey() );

    return buffer;
  }

  private void putPrincipalName( ByteBuffer buffer, String principalName )
  {
    String[] split = principalName.split("@");
    String nameComponent = split[0];
    String realm = split[1];

    String[] nameComponents = nameComponent.split( "/" );

    // increment for v1
    buffer.putShort( ( short ) nameComponents.length );

    putCountedString( buffer, realm );
    // write components

    for ( int ii = 0; ii < nameComponents.length; ii++ )
    {
      putCountedString( buffer, nameComponents[ii] );
    }
  }

  private void putKeyBlock( ByteBuffer buffer, EncryptionKey key )
  {
    buffer.putShort( ( short ) key.getKeyType().getValue() );
    putCountedBytes( buffer, key.getKeyValue() );
  }

  private void putCountedString( ByteBuffer buffer, String string )
  {
    byte[] data = string.getBytes();
    buffer.putShort( ( short ) data.length );
    buffer.put( data );
  }

  private void putCountedBytes( ByteBuffer buffer, byte[] data )
  {
    buffer.putShort( ( short ) data.length );
    buffer.put( data );
  }

}