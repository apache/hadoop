/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
#ifndef HADOOP_GROUP_INFO_DOT_H
#define HADOOP_GROUP_INFO_DOT_H

#include <grp.h> /* for struct group */
#include <unistd.h> /* for size_t */

struct hadoop_group_info {
  size_t buf_sz;
  struct group group;
  char *buf;
};

/**
 * Allocate a hadoop group info context.
 *
 * @return                        NULL on OOM; the context otherwise.
 */
struct hadoop_group_info *hadoop_group_info_alloc(void);

/**
 * Free a hadoop group info context.
 *
 * @param uinfo                   The hadoop group info context to free.
 */
void hadoop_group_info_free(struct hadoop_group_info *ginfo);

/**
 * Look up information for a group id.
 *
 * @param uinfo                   The hadoop group info context.
 *                                Existing data in this context will be cleared.
 * @param gid                     The group id to look up.
 *
 * @return                        ENOENT if the group wasn't found;
 *                                0 on success;
 *                                EIO, EMFILE, ENFILE, or ENOMEM if appropriate.
 */
int hadoop_group_info_fetch(struct hadoop_group_info *ginfo, gid_t gid);

#endif
