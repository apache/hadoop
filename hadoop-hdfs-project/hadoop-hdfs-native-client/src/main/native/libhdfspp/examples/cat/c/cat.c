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

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "hdfspp/hdfs_ext.h"

#define SCHEME "hdfs"
#define MAX_STRING 1024

struct Uri {
  int valid;
  char host[MAX_STRING];
  int port;
  char path[MAX_STRING];
};

int min(int a, int b) {
    return a < b ? a : b;
}

void parse_uri(const char * uri_string, struct Uri * uri) {
    uri->valid = 0;
    uri->host[0] = 0;
    uri->port = -1;
    uri->path[0] = 0;

    // most start with hdfs scheme
    const char * remaining;
    const char * scheme_end = strstr(uri_string, "://");
    if (scheme_end != NULL) {
      if (strncmp(uri_string, SCHEME, strlen(SCHEME)) != 0)
        return;

      remaining = scheme_end + 3;

      // parse authority
      const char * authority_end = strstr(remaining, "/");
      if (authority_end != NULL) {
        char authority[MAX_STRING];
        strncpy(authority, remaining, min(authority_end - remaining, sizeof(authority)));
        remaining = authority_end;

        char * host_port_separator = strstr(authority, ":");
        if (host_port_separator != NULL) {
          uri->port = strtol(host_port_separator + 1, NULL, 10);
          if (errno != 0)
            return;

          // Terminate authority at the new end of the host
          *host_port_separator = 0;
        }
        strncpy(uri->host, authority, sizeof(uri->host));
      }
      strncpy(uri->path, remaining, sizeof(uri->path));
    } else {
      // Absolute path
      strncpy(uri->path, uri_string, sizeof(uri->path));
    }

    uri->valid = 1;
};

int main(int argc, char** argv) {
  char error_text[1024];
  if (argc != 2) {
    fprintf(stderr, "usage: cat [hdfs://[<hostname>:<port>]]/<path-to-file>\n");
    return 1;
  }

  const char * uri_path = argv[1];
  struct Uri uri;
  parse_uri(uri_path, &uri);
  if (!uri.valid) {
    fprintf(stderr, "malformed URI: %s\n", uri_path);
    return 1;
  }

  struct hdfsBuilder* builder = hdfsNewBuilder();
  if (*uri.host != 0)
    hdfsBuilderSetNameNode(builder, uri.host);
  if (uri.port != -1)
    hdfsBuilderSetNameNodePort(builder, uri.port);

  hdfsFS fs = hdfsBuilderConnect(builder);
  if (fs == NULL) {
    hdfsGetLastError(error_text, sizeof(error_text));
    const char * host = uri.host[0] ? uri.host : "<default>";
    int port = uri.port;
    if (-1 == port)
      port = 8020;
    fprintf(stderr, "Unable to connect to %s:%d, hdfsConnect returned null.\n%s\n",
            host, port, error_text);
    return 1;
  }

  hdfsFile file = hdfsOpenFile(fs, uri.path, 0, 0, 0, 0);
  if (NULL == file) {
    hdfsGetLastError(error_text, sizeof(error_text));
    fprintf(stderr, "Unable to open file %s: %s\n", uri.path, error_text );
    hdfsDisconnect(fs);
    hdfsFreeBuilder(builder);
    return 1;
  }

  char input_buffer[4096];

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
  return 0;
}
