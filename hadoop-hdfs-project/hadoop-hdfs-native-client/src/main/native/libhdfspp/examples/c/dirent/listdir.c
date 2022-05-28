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

#include <errno.h>
#include <stdio.h>

#include "x-platform/c-api/dirent.h"

int main(int argc, char **argv) {
  const char *path = "/tmp/some-test-dir";
  const DIR *dir = opendir(path);
  if (dir == NULL) {
    printf("Unable to open the directory: %s, error: %d\n", path, errno);
    return 1;
  }

  if (closedir(dir) != 0) {
    printf("Unable to close the directory: %s, error: %d\n", path, errno);
  }

  return 0;
}
