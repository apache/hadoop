/*
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

#ifndef NATIVETASK_H_
#define NATIVETASK_H_

#include "lib/jniutils.h"
#include <stdint.h>
#include <string>
#include <vector>
#include <map>

namespace NativeTask {

using std::string;
using std::vector;
using std::map;
using std::pair;

/**
 * NativeObjectType
 */
enum NativeObjectType {
  UnknownObjectType = 0,
  BatchHandlerType = 1,
};

/**
 * Enduim setting
 *
 */
enum Endium {
  LITTLE_ENDIUM = 0,
  LARGE_ENDIUM = 1
};

#define NATIVE_COMBINER "native.combiner.class"
#define NATIVE_PARTITIONER "native.partitioner.class"
#define NATIVE_MAPPER "native.mapper.class"
#define NATIVE_RECORDREADER "native.recordreader.class"
#define NATIVE_RECORDWRITER "native.recordwriter.class"

#define NATIVE_REDUCER "native.reducer.class"
#define NATIVE_HADOOP_VERSION "native.hadoop.version"

#define NATIVE_INPUT_SPLIT "native.input.split"
#define INPUT_LINE_KV_SEPERATOR "mapreduce.input.keyvaluelinerecordreader.key.value.separator"
#define MAPRED_TEXTOUTPUT_FORMAT_SEPERATOR "mapreduce.output.textoutputformat.separator"
#define MAPRED_WORK_OUT_DIR "mapreduce.task.output.dir"
#define MAPRED_COMPRESS_OUTPUT "mapreduce.output.fileoutputformat.compress"
#define MAPRED_OUTPUT_COMPRESSION_CODEC "mapreduce.output.fileoutputformat.compress.codec"
#define TOTAL_ORDER_PARTITIONER_PATH "total.order.partitioner.path"
#define TOTAL_ORDER_PARTITIONER_MAX_TRIE_DEPTH "total.order.partitioner.max.trie.depth"
#define FS_DEFAULT_NAME "fs.default.name"
#define FS_DEFAULT_FS "fs.defaultFS"

#define NATIVE_SORT_TYPE "native.sort.type"
#define MAPRED_SORT_AVOID "mapreduce.sort.avoidance"
#define NATIVE_SORT_MAX_BLOCK_SIZE "native.sort.blocksize.max"
#define MAPRED_COMPRESS_MAP_OUTPUT "mapreduce.map.output.compress"
#define MAPRED_MAP_OUTPUT_COMPRESSION_CODEC "mapreduce.map.output.compress.codec"
#define MAPRED_MAPOUTPUT_KEY_CLASS "mapreduce.map.output.key.class"
#define MAPRED_OUTPUT_KEY_CLASS "mapreduce.job.output.key.class"
#define MAPRED_MAPOUTPUT_VALUE_CLASS "mapreduce.map.output.value.class"
#define MAPRED_OUTPUT_VALUE_CLASS "mapreduce.job.output.value.class"
#define MAPRED_IO_SORT_MB "mapreduce.task.io.sort.mb"
#define MAPRED_NUM_REDUCES "mapreduce.job.reduces"
#define MAPRED_COMBINE_CLASS_OLD "mapred.combiner.class"
#define MAPRED_COMBINE_CLASS_NEW "mapreduce.job.combine.class"

#define NATIVE_LOG_DEVICE "native.log.device"

//format: name=path,name=path,name=path
#define NATIVE_CLASS_LIBRARY_BUILDIN "native.class.library.buildin"

#define NATIVE_MAPOUT_KEY_COMPARATOR "native.map.output.key.comparator"

extern const std::string NativeObjectTypeToString(NativeObjectType type);
extern NativeObjectType NativeObjectTypeFromString(const std::string type);

/**
 * Objects that can be loaded dynamically from shared library,
 * and managed by NativeObjectFactory
 */
class NativeObject {
public:
  virtual NativeObjectType type() {
    return UnknownObjectType;
  }

  virtual ~NativeObject() {
  }
  ;
};

template<typename T>
NativeObject * ObjectCreator() {
  return new T();
}

typedef NativeObject * (*ObjectCreatorFunc)();

typedef ObjectCreatorFunc (*GetObjectCreatorFunc)(const std::string & name);

typedef void * (*FunctionGetter)(const std::string & name);

typedef int32_t (*InitLibraryFunc)();

/**
 * Exceptions
 */
class HadoopException : public std::exception {
private:
  std::string _reason;
public:
  HadoopException(const string & what);
  virtual ~HadoopException() throw () {
  }

  virtual const char* what() const throw () {
    return _reason.c_str();
  }
};

class OutOfMemoryException : public HadoopException {
public:
  OutOfMemoryException(const string & what)
      : HadoopException(what) {
  }
};

class IOException : public HadoopException {
public:
  IOException(const string & what)
      : HadoopException(what) {
  }
};

class UnsupportException : public HadoopException {
public:
  UnsupportException(const string & what)
      : HadoopException(what) {
  }
};

/**
 * Exception when call java methods using JNI
 */
class JavaException : public HadoopException {
public:
  JavaException(const string & what)
      : HadoopException(what) {
  }
};

#define STRINGIFY(x) #x
#define TOSTRING(x) STRINGIFY(x)
#define AT __FILE__ ":" TOSTRING(__LINE__)
#define THROW_EXCEPTION(type, what) throw type((std::string(AT":") + what))
#define THROW_EXCEPTION_EX(type, fmt, args...) \
        throw type(StringUtil::Format("%s:" fmt, AT, ##args))

class Config {
protected:
  map<string, string> _configs;
public:
  Config() {
  }
  ~Config() {
  }

  const char * get(const string & name);

  string get(const string & name, const string & defaultValue);

  bool getBool(const string & name, bool defaultValue);

  int64_t getInt(const string & name, int64_t defaultValue = -1);

  float getFloat(const string & name, float defaultValue = -1);

  void getStrings(const string & name, vector<string> & dest);

  void getInts(const string & name, vector<int64_t> & dest);

  void getFloats(const string & name, vector<float> & dest);

  void set(const string & key, const string & value);

  void setInt(const string & name, int64_t value);

  void setBool(const string & name, bool value);

  /**
   * Load configs from a config file with the following format:
   * # comment
   * key1=value1
   * key2=value2
   * ...
   */
  void load(const string & path);

  /**
   * Load configs form command line args
   * key1=value1 key2=value2,value2
   */
  void parse(int32_t argc, const char ** argv);
};

class Command {
private:
  int _id;
  const char * _description;

public:
  Command(int id, const char * description)
      : _id(id), _description(description) {
  }

  Command(int id)
      : _id(id), _description(NULL) {
  }

  int id() const {
    return _id;
  }

  const char * description() const {
    return _description;
  }

  bool equals(const Command & other) const {
    if (_id == other._id) {
      return true;
    }
    return false;
  }
};

class Buffer {
protected:
  const char * _data;
  uint32_t _length;

public:
  Buffer()
      : _data(NULL), _length(0) {
  }

  Buffer(const char * data, uint32_t length)
      : _data(data), _length(length) {
  }

  ~Buffer() {
  }

  void reset(const char * data, uint32_t length) {
    this->_data = data;
    this->_length = length;
  }

  const char * data() const {
    return _data;
  }

  uint32_t length() const {
    return _length;
  }

  void data(const char * data) {
    this->_data = data;
  }

  void length(uint32_t length) {
    this->_length = length;
  }

  string toString() const {
    return string(_data, _length);
  }
};

class InputSplit {
public:
  virtual uint64_t getLength() = 0;
  virtual vector<string> & getLocations() = 0;
  virtual void readFields(const string & data) = 0;
  virtual void writeFields(string & dest) = 0;
  virtual string toString() = 0;

  virtual ~InputSplit() {

  }
};

class Configurable : public NativeObject {
public:
  Configurable() {
  }

  virtual void configure(Config * config) {
  }
};

class Collector {
public:
  virtual ~Collector() {
  }

  virtual void collect(const void * key, uint32_t keyLen, const void * value, uint32_t valueLen) {
  }

  virtual void collect(const void * key, uint32_t keyLen, const void * value, uint32_t valueLen,
      int32_t partition) {
    collect(key, keyLen, value, valueLen);
  }
};

class Progress {
public:
  virtual ~Progress() {
  }
  virtual float getProgress() = 0;
};

class Counter {
private:
  // not thread safe
  // TODO: use atomic
  volatile uint64_t _count;

  string _group;
  string _name;
public:
  Counter(const string & group, const string & name)
      : _count(0), _group(group), _name(name) {
  }

  const string & group() const {
    return _group;
  }
  const string & name() const {
    return _name;
  }

  uint64_t get() const {
    return _count;
  }

  void increase() {
    _count++;
  }

  void increase(uint64_t cnt) {
    _count += cnt;
  }
};

class KVIterator {
public:
  virtual ~KVIterator() {
  }
  virtual bool next(Buffer & key, Buffer & value) = 0;
};


class ProcessorBase : public Configurable {
protected:
  Collector * _collector;
public:
  ProcessorBase()
      : _collector(NULL) {
  }

  void setCollector(Collector * collector) {
    _collector = collector;
  }

  Collector * getCollector() {
    return _collector;
  }

  void collect(const void * key, uint32_t keyLen, const void * value, uint32_t valueLen) {
    _collector->collect(key, keyLen, value, valueLen);
  }

  void collect(const void * key, uint32_t keyLen, const void * value, uint32_t valueLen,
      int32_t partition) {
    _collector->collect(key, keyLen, value, valueLen, partition);
  }

  Counter * getCounter(const string & group, const string & name);

  virtual void close() {
  }
};

enum KeyGroupIterState {
  SAME_KEY,
  NEW_KEY,
  NEW_KEY_VALUE,
  NO_MORE,
};

class KeyGroupIterator {
public:
  virtual ~KeyGroupIterator() {
  }
  /**
   * Move to nextKey, or begin this iterator
   */
  virtual bool nextKey() = 0;

  /**
   * Get key of this input group
   */
  virtual const char * getKey(uint32_t & len) = 0;

  /**
   * Get next value of this input group
   * @return NULL if no more
   */
  virtual const char * nextValue(uint32_t & len) = 0;
};



enum KeyValueType {
  TextType = 0,
  BytesType = 1,
  ByteType = 2,
  BoolType = 3,
  IntType = 4,
  LongType = 5,
  FloatType = 6,
  DoubleType = 7,
  MD5HashType = 8,
  VIntType = 9,
  VLongType = 10,
  UnknownType = -1
};

typedef int (*ComparatorPtr)(const char * src, uint32_t srcLength, const char * dest,
    uint32_t destLength);

ComparatorPtr get_comparator(const KeyValueType keyType, const char * comparatorName);

typedef void (*ANY_FUNC_PTR)();

} // namespace NativeTask;

/**
 * Use these two predefined macro to define a class library:
 *   DEFINE_NATIVE_LIBRARY(Library)
 *   REGISTER_CLASS(Type, Library)
 * For example, suppose we have a demo application, which has
 * defined class MyDemoMapper and MyDemoReducer, to register
 * this module & these two classes, you need to add following
 * code to you source code.
 *   DEFINE_NATIVE_LIBRARY(MyDemo) {
 *     REGISTER_CLASS(MyDemoMapper, MyDemo);
 *     REGISTER_CLASS(MyDemoReducer, MyDemo);
 *   }
 * The class name for MyDemoMapper will be MyDemo.MyDemoMapper,
 * and similar for MyDemoReducer.
 * Then you can set native.mapper.class to MyDemo.MyDemoMapper
 * in JobConf.
 */

#define DEFINE_NATIVE_LIBRARY(Library) \
  static std::map<std::string, NativeTask::ObjectCreatorFunc> Library##ClassMap__; \
  extern "C" void * Library##GetFunctionGetter(const std::string & name) { \
      std::map<std::string, NativeTask::ObjectCreatorFunc>::iterator itr = Library##ClassMap__.find(name); \
      if (itr != Library##ClassMap__.end()) { \
        return (void *)(itr->second); \
      } \
      return NULL; \
    } \
  extern "C" NativeTask::ObjectCreatorFunc Library##GetObjectCreator(const std::string & name) { \
    std::map<std::string, NativeTask::ObjectCreatorFunc>::iterator itr = Library##ClassMap__.find(name); \
    if (itr != Library##ClassMap__.end()) { \
      return itr->second; \
    } \
    return NULL; \
  } \
  extern "C" void Library##Init()

#define REGISTER_CLASS(Type, Library) Library##ClassMap__[#Library"."#Type] = NativeTask::ObjectCreator<Type>

#define REGISTER_FUNCTION(Type, Library) Library##ClassMap__[#Library"."#Type] = (ObjectCreatorFunc)Type

#endif /* NATIVETASK_H_ */
