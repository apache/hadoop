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

#ifndef _FCHMODAT_H_
#define _FCHMODAT_H_

#include <sys/stat.h>

#include <unistd.h>

#define AT_SYMLINK_NOFOLLOW 0x01

static int
fchmodat(int fd, const char *path, mode_t mode, int flag)
{
  int cfd, error, ret;

  cfd = open(".", O_RDONLY | O_DIRECTORY);
  if (cfd == -1)
    return (-1);

  if (fchdir(fd) == -1) {
    error = errno;
    (void)close(cfd);
    errno = error;
    return (-1);
  }

  if (flag == AT_SYMLINK_NOFOLLOW)
    ret = lchmod(path, mode);
  else
    ret = chmod(path, mode);

  error = errno;
  (void)fchdir(cfd);
  (void)close(cfd);
  errno = error;
  return (ret);
}

#endif  /* !_FCHMODAT_H_ */
