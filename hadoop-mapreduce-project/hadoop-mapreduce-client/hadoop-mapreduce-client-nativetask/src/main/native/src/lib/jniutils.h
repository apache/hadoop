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

#ifndef JNIUTILS_H_
#define JNIUTILS_H_

#include <string>
#include <jni.h>

/**
 * Get current JavaVM, if none then try to create one.
 */
JavaVM * JNU_GetJVM(void);

/**
 * Get JNIEnv for current thread.
 */
JNIEnv* JNU_GetJNIEnv(void);

/**
 * Attach currentThread, same effect as JNU_GetJNIEnv.
 */
void JNU_AttachCurrentThread();

/**
 * Detach current thread, call it if current thread
 * is created in native side and have called
 * JNU_AttachCurrentThread before
 */
void JNU_DetachCurrentThread();

/**
 * Throw a java exception.
 */
void JNU_ThrowByName(JNIEnv *jenv, const char *name, const char *msg);

/**
 * Convert a java byte array to c++ std::string
 */
std::string JNU_ByteArrayToString(JNIEnv * jenv, jbyteArray src);

#endif /* JNIUTILS_H_ */
