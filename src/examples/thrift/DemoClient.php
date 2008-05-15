<?php
/**
 * Copyright 2008 The Apache Software Foundation
 * 
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

# Change this to match your thrift root
$GLOBALS['THRIFT_ROOT'] = dirname(__FILE__).'/thrift';

require_once( $GLOBALS['THRIFT_ROOT'].'/Thrift.php' );

require_once( $GLOBALS['THRIFT_ROOT'].'/transport/TSocket.php' );
require_once( $GLOBALS['THRIFT_ROOT'].'/transport/TBufferedTransport.php' );
require_once( $GLOBALS['THRIFT_ROOT'].'/protocol/TBinaryProtocol.php' );

# According to the thrift documentation, compiled PHP thrift libraries should
# reside under the THRIFT_ROOT/packages directory.
require_once( $GLOBALS['THRIFT_ROOT'].'/packages/Hbase/Hbase.php' );

function printRow( $row, $values ) {
  echo( "row: {$row}, cols: \n" );
  asort( $values );
  foreach ( $values as $k=>$v ) {
    echo( "  {$k} => {$v}\n" );
  }
}

function printEntry( $entry ) {
  printRow( $entry->row, $entry->columns );
}

$socket = new TSocket( 'localhost', 9090 );
$socket->setSendTimeout( 10000 ); // Ten seconds (too long for production, but this is just a demo ;)
$socket->setRecvTimeout( 20000 ); // Twenty seconds
$transport = new TBufferedTransport( $socket );
$protocol = new TBinaryProtocol( $transport );
$client = new HbaseClient( $protocol );

$transport->open();

$t = 'demo_table';

?><html>
<head>
<title>DemoClient</title>
</head>
<body>
<pre>
<?php

#
# Scan all tables, look for the demo table and delete it.
#
echo( "scanning tables...\n" );
$tables = $client->getTableNames();
sort( $tables );
foreach ( $tables as $name ) {
  echo( "  found: {$name}\n" );
  if ( $name == $t ) {
    echo( "    deleting table: {$name}\n" );
    $client->deleteTable( $name );
  }
}

#
# Create the demo table with two column families, entry: and unused:
#
$columns = array(
  new ColumnDescriptor( array(
    'name' => 'entry:',
    'maxVersions' => 10
  ) ),
  new ColumnDescriptor( array(
    'name' => 'unused:'
  ) )
);

echo( "creating table: {$t}\n" );
try {
  $client->createTable( $t, $columns );
} catch ( AlreadyExists $ae ) {
  echo( "WARN: {$ae->message}\n" );
}

echo( "column families in {$t}:\n" );
$descriptors = $client->getColumnDescriptors( $t );
asort( $descriptors );
foreach ( $descriptors as $col ) {
  echo( "  column: {$col->name}, maxVer: {$col->maxVersions}\n" );
}

#
# Test UTF-8 handling
#
$invalid = "foo-\xfc\xa1\xa1\xa1\xa1\xa1";
$valid = "foo-\xE7\x94\x9F\xE3\x83\x93\xE3\x83\xBC\xE3\x83\xAB";

# non-utf8 is fine for data
$client->put( $t, "foo", "entry:foo", $invalid );

# try empty strings
$client->put( $t, "", "entry:", "" );

# this row name is valid utf8
$client->put( $t, $valid, "entry:foo", $valid );

# non-utf8 is not allowed in row names
try {
  $client->put( $t, $invalid, "entry:foo", $invalid );
  throw new Exception( "shouldn't get here!" );
} catch ( IOError $e ) {
  echo( "expected error: {$e->message}\n" );
}

# Run a scanner on the rows we just created
echo( "Starting scanner...\n" );
$scanner = $client->scannerOpen( $t, "", array( "entry:" ) );
try {
  while (true) printEntry( $client->scannerGet( $scanner ) );
} catch ( NotFound $nf ) {
  $client->scannerClose( $scanner );
  echo( "Scanner finished\n" );
}

#
# Run some operations on a bunch of rows.
#
for ($e=100; $e>=0; $e--) {

  # format row keys as "00000" to "00100"
  $row = str_pad( $e, 5, '0', STR_PAD_LEFT );

  $client->put( $t, $row, "unused:", "DELETE_ME" );
  printRow( $row, $client->getRow( $t, $row ) );
  $client->deleteAllRow( $t, $row );

  $client->put( $t, $row, "entry:num", "0" );
  $client->put( $t, $row, "entry:foo", "FOO");
  printRow( $row, $client->getRow( $t, $row ) );

  $mutations = array(
    new Mutation( array(
      'column' => 'entry:foo',
      'isDelete' => 1
    ) ),
    new Mutation( array(
      'column' => 'entry:num',
      'value' => '-1'
    ) ),
  );
  $client->mutateRow( $t, $row, $mutations );
  printRow( $row, $client->getRow( $t, $row ) );

  $client->put( $t, $row, "entry:num", $e );
  $client->put( $t, $row, "entry:sqr", $e * $e );
  printRow( $row, $client->getRow( $t, $row ) );
  
  $mutations = array(
    new Mutation( array(
      'column' => 'entry:num',
      'isDelete' => '-999'
    ) ),
    new Mutation( array(
      'column' => 'entry:sqr',
      'isDelete' => 1
    ) ),
  );
  $client->mutateRowTs( $t, $row, $mutations, 1 ); # shouldn't override latest
  printRow( $row, $client->getRow( $t, $row ) );

  $versions = $client->getVer( $t, $row, "entry:num", 10 );
  echo( "row: {$row}, values: \n" );
  foreach ( $versions as $v ) echo( "  {$v};\n" );
  
  try {
    $client->get( $t, $row, "entry:foo");
    throw new Exception ( "shouldn't get here! " );
  } catch ( NotFound $nf ) {
    # blank
  }

}

$columns = array();
foreach ( $client->getColumnDescriptors($t) as $col=>$desc ) $columns[] = $col;

echo( "Starting scanner...\n" );
$scanner = $client->scannerOpenWithStop( $t, "00020", "00040", $columns );
try {
  while (true) printEntry( $client->scannerGet( $scanner ) );
} catch ( NotFound $nf ) {
  $client->scannerClose( $scanner );
  echo( "Scanner finished\n" );
}
  
$transport->close();

?>
</pre>
</body>
</html>

