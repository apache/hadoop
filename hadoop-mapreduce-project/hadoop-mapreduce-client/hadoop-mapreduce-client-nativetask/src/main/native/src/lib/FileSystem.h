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

#ifndef FILESYSTEM_H_
#define FILESYSTEM_H_

#include <string>
#include "NativeTask.h"
#include "lib/Streams.h"

namespace NativeTask {

class FileSystem;

/**
 * Local raw filesystem file input stream
 * with blocking semantics
 */
class FileInputStream : public InputStream {
private:
  string _path;
  int _fd;
  Counter * _bytesRead;
public:
  FileInputStream(const string & path);
  virtual ~FileInputStream();

  virtual void seek(uint64_t position);

  virtual uint64_t tell();

  virtual int32_t read(void * buff, uint32_t length);

  virtual void close();
};

/**
 * Local raw filesystem file output stream
 * with blocking semantics
 */
class FileOutputStream : public OutputStream {
private:
  string _path;
  int _fd;
  Counter * _bytesWrite;
public:
  FileOutputStream(const string & path, bool overwite = true);
  virtual ~FileOutputStream();

  virtual uint64_t tell();

  virtual void write(const void * buff, uint32_t length);

  virtual void flush();

  virtual void close();
};


class FileEntry {
public:
  string name;
  bool isDirectory;
};

/**
 * FileSystem interface
 */
class FileSystem {
protected:
  FileSystem() {
  }
public:
  virtual ~FileSystem() {
  }

  virtual InputStream * open(const string & path) {
    return NULL;
  }

  virtual OutputStream * create(const string & path, bool overwrite = true) {
    return NULL;
  }

  virtual uint64_t getLength(const string & path) {
    return 0;
  }

  virtual bool list(const string & path, vector<FileEntry> & status) {
    return false;
  }

  virtual void remove(const string & path) {
  }

  virtual bool exists(const string & path) {
    return false;
  }

  virtual void mkdirs(const string & path) {
  }

  static FileSystem & getLocal();
};

} // namespace NativeTask

#endif /* FILESYSTEM_H_ */
