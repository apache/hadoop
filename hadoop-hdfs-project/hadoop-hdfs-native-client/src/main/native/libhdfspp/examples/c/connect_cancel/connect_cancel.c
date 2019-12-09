/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
*/

/*
  Attempt to connect to a cluster and use Control-C to bail out if it takes a while.
  Valid config must be in environment variable $HADOOP_CONF_DIR
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>

#include "hdfspp/hdfs_ext.h"
#include "common/util_c.h"

#define ERROR_BUFFER_SIZE 1024

// Global so signal handler can get at it
hdfsFS fs = NULL;

const char *catch_enter  = "In signal handler, going to try and cancel.\n";
const char *catch_cancel = "hdfsCancelPendingConnect has been canceled in the signal handler.\n";
const char *catch_exit   = "Exiting the signal handler.\n";

// Print to stdout without calling malloc or otherwise indirectly modify userspace state.
// Write calls to stdout may still interleave with stuff coming from elsewhere.
static void sighandler_direct_stdout(const char *msg) {
  if(!msg)
    return;
  ssize_t res = write(1 /*posix stdout fd*/, msg, strlen(msg));
  (void)res;
}

static void sig_catch(int val) {
  // Beware of calling things that aren't reentrant e.g. malloc while in a signal handler.
  sighandler_direct_stdout(catch_enter);

  if(fs) {
    hdfsCancelPendingConnection(fs);
    sighandler_direct_stdout(catch_cancel);
  }
  sighandler_direct_stdout(catch_exit);
}


int main(int argc, char** argv) {
  hdfsSetLoggingLevel(HDFSPP_LOG_LEVEL_INFO);
  signal(SIGINT, sig_catch);

  char error_text[ERROR_BUFFER_SIZE];
  if (argc != 1) {
    fprintf(stderr, "usage: ./connect_cancel_c\n");
    ShutdownProtobufLibrary_C();
    exit(EXIT_FAILURE);
  }

  const char *hdfsconfdir = getenv("HADOOP_CONF_DIR");
  if(!hdfsconfdir) {
    fprintf(stderr, "$HADOOP_CONF_DIR must be set\n");
    ShutdownProtobufLibrary_C();
    exit(EXIT_FAILURE);
  }

  struct hdfsBuilder* builder = hdfsNewBuilderFromDirectory(hdfsconfdir);

  fs = hdfsAllocateFileSystem(builder);
  if (fs == NULL) {
    hdfsGetLastError(error_text, ERROR_BUFFER_SIZE);
    fprintf(stderr, "hdfsAllocateFileSystem returned null.\n%s\n", error_text);
    hdfsFreeBuilder(builder);
    ShutdownProtobufLibrary_C();
    exit(EXIT_FAILURE);
  }

  int connected = hdfsConnectAllocated(fs, builder);
  if (connected != 0) {
    hdfsGetLastError(error_text, ERROR_BUFFER_SIZE);
    fprintf(stderr, "hdfsConnectAllocated errored.\n%s\n", error_text);
    hdfsFreeBuilder(builder);
    ShutdownProtobufLibrary_C();
    exit(EXIT_FAILURE);
  }

  hdfsDisconnect(fs);
  hdfsFreeBuilder(builder);
  // Clean up static data and prevent valgrind memory leaks
  ShutdownProtobufLibrary_C();
  return 0;
}
