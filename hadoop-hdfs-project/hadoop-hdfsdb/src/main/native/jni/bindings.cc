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
#include <jni.h>

#undef JNIEXPORT
#if _WIN32
#define JNIEXPORT __declspec(dllexport)
#else
#define JNIEXPORT __attribute__((visibility("default")))
#endif

#include "org_apache_hadoop_hdfs_hdfsdb_DB.h"
#include "org_apache_hadoop_hdfs_hdfsdb_Iterator.h"
#include "org_apache_hadoop_hdfs_hdfsdb_Options.h"
#include "org_apache_hadoop_hdfs_hdfsdb_NativeObject.h"
#include "org_apache_hadoop_hdfs_hdfsdb_ReadOptions.h"
#include "org_apache_hadoop_hdfs_hdfsdb_WriteBatch.h"
#include "org_apache_hadoop_hdfs_hdfsdb_WriteOptions.h"

#include <leveldb/db.h>
#include <leveldb/options.h>
#include <leveldb/write_batch.h>
#include <leveldb/cache.h>

static inline uintptr_t uintptr(void *ptr) {
  return reinterpret_cast<uintptr_t>(ptr);
}

static inline jbyteArray ToJByteArray(JNIEnv *env, const leveldb::Slice &slice) {
  jbyteArray res = env->NewByteArray(slice.size());
  if (!res) {
    env->ThrowNew(env->FindClass("java/lang/OutOfMemoryError"), "");
  } else {
    env->SetByteArrayRegion(res, 0, slice.size(), reinterpret_cast<const jbyte*>(slice.data()));
  }
  return res;
}

struct GetByteArrayElements {
  static jbyte *Get(JNIEnv *env, jbyteArray array) {
    return env->GetByteArrayElements(array, NULL);
  }
  static void Release(JNIEnv *env, jbyteArray array, jbyte *data) {
    env->ReleaseByteArrayElements(array, data, JNI_ABORT);
  }
};

struct GetByteArrayCritical {
  static jbyte *Get(JNIEnv *env, jbyteArray array) {
    return reinterpret_cast<jbyte*>(env->GetPrimitiveArrayCritical(array, NULL));
  }
  static void Release(JNIEnv *env, jbyteArray array, jbyte *data) {
    env->ReleasePrimitiveArrayCritical(array, data, JNI_ABORT);
  }
};

template <class Trait>
class JNIByteArrayHolder {
 public:
  JNIByteArrayHolder(JNIEnv *env, jbyteArray array)
      : env_(env)
      , array_(array)
  {
    data_ = Trait::Get(env, array);
    length_ = env_->GetArrayLength(array);
  }

  ~JNIByteArrayHolder() { Trait::Release(env_, array_, data_); }
  const char *data() const { return reinterpret_cast<const char*>(data_); }
  int length() const { return length_; }
  const leveldb::Slice slice() const { return leveldb::Slice(data(), length()); }

 private:
  JNIEnv *env_;
  jbyteArray array_;
  jbyte *data_;
  int length_;
};


jlong JNICALL Java_org_apache_hadoop_hdfs_hdfsdb_DB_open(JNIEnv *env, jclass, jlong joptions, jstring path) {
  leveldb::DB *db = NULL;
  const char *path_str = env->GetStringUTFChars(path, 0);
  leveldb::Options *options = reinterpret_cast<leveldb::Options*>(joptions);
  leveldb::Status status = leveldb::DB::Open(*options, path_str, &db);
  env->ReleaseStringUTFChars(path, path_str);
  if (!status.ok()) {
    env->ThrowNew(env->FindClass("java/io/IOException"), status.ToString().c_str());
  }
  return uintptr(db);
}

void JNICALL Java_org_apache_hadoop_hdfs_hdfsdb_DB_close(JNIEnv *, jclass, jlong handle) {
  leveldb::DB *db = reinterpret_cast<leveldb::DB*>(handle);
  delete db;
}

jbyteArray JNICALL Java_org_apache_hadoop_hdfs_hdfsdb_DB_get(JNIEnv *env, jclass, jlong handle, jlong jread_options, jbyteArray jkey) {
  leveldb::DB *db = reinterpret_cast<leveldb::DB*>(handle);
  leveldb::ReadOptions *options = reinterpret_cast<leveldb::ReadOptions*>(jread_options);
  std::string result;
  leveldb::Status status;
  {
    JNIByteArrayHolder<GetByteArrayElements> key(env, jkey);
    status = db->Get(*options, key.slice(), &result);
  }

  if (status.IsNotFound()) {
    return NULL;
  } else if (!status.ok()) {
    env->ThrowNew(env->FindClass("java/io/IOException"), status.ToString().c_str());
    return NULL;
  }

  return ToJByteArray(env, leveldb::Slice(result));
}

void JNICALL Java_org_apache_hadoop_hdfs_hdfsdb_DB_write(JNIEnv *env, jclass, jlong handle, jlong jwrite_options, jlong jbatch) {
  leveldb::DB *db = reinterpret_cast<leveldb::DB*>(handle);
  leveldb::WriteOptions *options = reinterpret_cast<leveldb::WriteOptions*>(jwrite_options);
  leveldb::WriteBatch *batch = reinterpret_cast<leveldb::WriteBatch*>(jbatch);
  leveldb::Status status = db->Write(*options, batch);
}

void JNICALL Java_org_apache_hadoop_hdfs_hdfsdb_DB_delete(JNIEnv *env, jclass, jlong handle, jlong jwrite_options, jbyteArray jkey) {
  leveldb::DB *db = reinterpret_cast<leveldb::DB*>(handle);
  leveldb::WriteOptions *options = reinterpret_cast<leveldb::WriteOptions*>(jwrite_options);
  leveldb::Status status;
  {
    JNIByteArrayHolder<GetByteArrayElements> key(env, jkey);
    status = db->Delete(*options, key.slice());
  }
}

jlong JNICALL Java_org_apache_hadoop_hdfs_hdfsdb_DB_newIterator(JNIEnv *, jclass, jlong handle, jlong jread_options) {
  leveldb::DB *db = reinterpret_cast<leveldb::DB*>(handle);
  leveldb::ReadOptions *options = reinterpret_cast<leveldb::ReadOptions*>(jread_options);
  auto res = uintptr(db->NewIterator(*options));
  return res;
}

void JNICALL Java_org_apache_hadoop_hdfs_hdfsdb_Iterator_destruct(JNIEnv *, jclass, jlong handle) {
  delete reinterpret_cast<leveldb::Iterator*>(handle);
}

void JNICALL Java_org_apache_hadoop_hdfs_hdfsdb_Iterator_seek(JNIEnv *env, jclass, jlong handle, jbyteArray jkey) {
  leveldb::Iterator *it = reinterpret_cast<leveldb::Iterator*>(handle);
  JNIByteArrayHolder<GetByteArrayElements> key(env, jkey);
  it->Seek(key.slice());
}

void JNICALL Java_org_apache_hadoop_hdfs_hdfsdb_Iterator_next(JNIEnv *, jclass, jlong handle) {
  leveldb::Iterator *it = reinterpret_cast<leveldb::Iterator*>(handle);
  it->Next();
}

jboolean JNICALL Java_org_apache_hadoop_hdfs_hdfsdb_Iterator_valid(JNIEnv *, jclass, jlong handle) {
  leveldb::Iterator *it = reinterpret_cast<leveldb::Iterator*>(handle);
  return it->Valid();
}

jbyteArray JNICALL Java_org_apache_hadoop_hdfs_hdfsdb_Iterator_key(JNIEnv *env, jclass, jlong handle) {
  leveldb::Iterator *it = reinterpret_cast<leveldb::Iterator*>(handle);
  return ToJByteArray(env, it->key());
}

jbyteArray JNICALL Java_org_apache_hadoop_hdfs_hdfsdb_Iterator_value(JNIEnv *env, jclass, jlong handle) {
  leveldb::Iterator *it = reinterpret_cast<leveldb::Iterator*>(handle);
  return ToJByteArray(env, it->value());
}

jlong JNICALL Java_org_apache_hadoop_hdfs_hdfsdb_Options_construct(JNIEnv *, jclass) {
  return uintptr(new leveldb::Options());
}

void JNICALL Java_org_apache_hadoop_hdfs_hdfsdb_Options_destruct(JNIEnv *, jclass, jlong handle) {
  delete reinterpret_cast<leveldb::Options*>(handle);
}

void JNICALL Java_org_apache_hadoop_hdfs_hdfsdb_Options_createIfMissing(JNIEnv *, jclass, jlong handle, jboolean value) {
  leveldb::Options *options = reinterpret_cast<leveldb::Options*>(handle);
  options->create_if_missing = value;
}

void JNICALL Java_org_apache_hadoop_hdfs_hdfsdb_Options_compressionType(JNIEnv *, jclass, jlong handle, jint value) {
  leveldb::Options *options = reinterpret_cast<leveldb::Options*>(handle);
  options->compression = (leveldb::CompressionType)value;
}

void JNICALL Java_org_apache_hadoop_hdfs_hdfsdb_Options_writeBufferSize(JNIEnv *, jclass, jlong handle, jint value) {
  leveldb::Options *options = reinterpret_cast<leveldb::Options*>(handle);
  options->write_buffer_size = value;
}

void JNICALL Java_org_apache_hadoop_hdfs_hdfsdb_Options_blockSize(JNIEnv *, jclass, jlong handle, jint value) {
  leveldb::Options *options = reinterpret_cast<leveldb::Options*>(handle);
  options->block_size = value;
}

jlong JNICALL Java_org_apache_hadoop_hdfs_hdfsdb_ReadOptions_construct(JNIEnv *, jclass) {
  return uintptr(new leveldb::ReadOptions());
}

void JNICALL Java_org_apache_hadoop_hdfs_hdfsdb_ReadOptions_destruct(JNIEnv *, jclass, jlong handle) {
  delete reinterpret_cast<leveldb::ReadOptions*>(handle);
}

jlong JNICALL Java_org_apache_hadoop_hdfs_hdfsdb_WriteOptions_construct(JNIEnv *, jclass) {
  return uintptr(new leveldb::WriteOptions());
}

void JNICALL Java_org_apache_hadoop_hdfs_hdfsdb_WriteOptions_sync(JNIEnv *, jclass, jlong handle, jboolean value) {
  leveldb::WriteOptions *options = reinterpret_cast<leveldb::WriteOptions*>(handle);
  options->sync = value;
}

void JNICALL Java_org_apache_hadoop_hdfs_hdfsdb_WriteOptions_destruct(JNIEnv *, jclass, jlong handle) {
  delete reinterpret_cast<leveldb::WriteOptions*>(handle);
}

jlong JNICALL Java_org_apache_hadoop_hdfs_hdfsdb_WriteBatch_construct(JNIEnv *, jclass) {
  return uintptr(new leveldb::WriteBatch());
}

void JNICALL Java_org_apache_hadoop_hdfs_hdfsdb_WriteBatch_destruct(JNIEnv *, jclass, jlong handle) {
  delete reinterpret_cast<leveldb::WriteBatch*>(handle);
}

void JNICALL Java_org_apache_hadoop_hdfs_hdfsdb_WriteBatch_put(JNIEnv *env, jclass, jlong handle, jbyteArray jkey, jbyteArray jvalue) {
  leveldb::WriteBatch *batch = reinterpret_cast<leveldb::WriteBatch*>(handle);
  JNIByteArrayHolder<GetByteArrayCritical> key(env, jkey), value(env, jvalue);
  batch->Put(key.slice(), value.slice());
}

void JNICALL Java_org_apache_hadoop_hdfs_hdfsdb_WriteBatch_delete(JNIEnv *env, jclass, jlong handle, jbyteArray jkey) {
  leveldb::WriteBatch *batch = reinterpret_cast<leveldb::WriteBatch*>(handle);
  JNIByteArrayHolder<GetByteArrayCritical> key(env, jkey);
  batch->Delete(key.slice());
}

void JNICALL Java_org_apache_hadoop_hdfs_hdfsdb_WriteBatch_clear(JNIEnv *, jclass, jlong handle) {
  leveldb::WriteBatch *batch = reinterpret_cast<leveldb::WriteBatch*>(handle);
  batch->Clear();
}
