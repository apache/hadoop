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
  A a stripped down version of unix's "cat".
  Doesn't deal with any flags for now, will just attempt to read the whole file.
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "hdfspp/hdfs_ext.h"
#include "uriparser2/uriparser2.h"
#include "common/util_c.h"

#define SCHEME "hdfs"
#define BUF_SIZE 1048576 //1 MB
static char input_buffer[BUF_SIZE];

int main(int argc, char** argv) {

  char error_text[1024];
  if (argc != 2) {
    fprintf(stderr, "usage: cat [hdfs://[<hostname>:<port>]]/<path-to-file>\n");
    return 1;
  }

  URI * uri = NULL;
  const char * uri_path = argv[1];

  //Separate check for scheme is required, otherwise uriparser2.h library causes memory issues under valgrind
  const char * scheme_end = strstr(uri_path, "://");
  if (scheme_end) {
    if (strncmp(uri_path, SCHEME, strlen(SCHEME)) != 0) {
      fprintf(stderr, "Scheme %.*s:// is not supported.\n", (int) (scheme_end - uri_path), uri_path);
      return 1;
    } else {
      uri = uri_parse(uri_path);
    }
  }
  if (!uri) {
    fprintf(stderr, "Malformed URI: %s\n", uri_path);
    return 1;
  }

  struct hdfsBuilder* builder = hdfsNewBuilder();
  if (uri->host)
    hdfsBuilderSetNameNode(builder, uri->host);
  if (uri->port != 0)
    hdfsBuilderSetNameNodePort(builder, uri->port);

  hdfsFS fs = hdfsBuilderConnect(builder);
  if (fs == NULL) {
    hdfsGetLastError(error_text, sizeof(error_text));
    const char * host = uri->host ? uri->host : "<default>";
    int port = uri->port;
    if (port == 0)
      port = 8020;
    fprintf(stderr, "Unable to connect to %s:%d, hdfsConnect returned null.\n%s\n",
            host, port, error_text);
    return 1;
  }

  hdfsFile file = hdfsOpenFile(fs, uri->path, 0, 0, 0, 0);
  if (NULL == file) {
    hdfsGetLastError(error_text, sizeof(error_text));
    fprintf(stderr, "Unable to open file %s: %s\n", uri->path, error_text );
    hdfsDisconnect(fs);
    hdfsFreeBuilder(builder);
    return 1;
  }

  ssize_t read_bytes_count = 0;
  ssize_t last_read_bytes = 0;

  while (0 < (last_read_bytes =
                  hdfsPread(fs, file, read_bytes_count, input_buffer, sizeof(input_buffer)))) {
    fwrite(input_buffer, last_read_bytes, 1, stdout);
    read_bytes_count += last_read_bytes;
  }

  int res = 0;
  res = hdfsCloseFile(fs, file);
  if (0 != res) {
    hdfsGetLastError(error_text, sizeof(error_text));
    fprintf(stderr, "Error closing file: %s\n", error_text);
    hdfsDisconnect(fs);
    hdfsFreeBuilder(builder);
    return 1;
  }

  res = hdfsDisconnect(fs);
  if (0 != res) {
    hdfsGetLastError(error_text, sizeof(error_text));
    fprintf(stderr, "Error disconnecting filesystem: %s", error_text);
    hdfsFreeBuilder(builder);
    return 1;
  }

  hdfsFreeBuilder(builder);
  free(uri);
  // Clean up static data and prevent valgrind memory leaks
  ShutdownProtobufLibrary_C();
  return 0;
}
