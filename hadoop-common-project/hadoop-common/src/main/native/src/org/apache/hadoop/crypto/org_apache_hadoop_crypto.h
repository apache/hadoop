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
 
#ifndef ORG_APACHE_HADOOP_CRYPTO_H
#define ORG_APACHE_HADOOP_CRYPTO_H

#include "org_apache_hadoop.h"

#ifdef UNIX
#include <dlfcn.h>
#include "config.h"
#endif

#ifdef WINDOWS
#include "winutils.h"
#endif

#include <openssl/aes.h>
#include <openssl/evp.h>
#include <openssl/err.h>

/**
 * A helper macro to convert the java 'context-handle' 
 * to a EVP_CIPHER_CTX pointer. 
 */
#define CONTEXT(context) ((EVP_CIPHER_CTX*)((ptrdiff_t)(context)))

/**
 * A helper macro to convert the EVP_CIPHER_CTX pointer to the 
 * java 'context-handle'.
 */
#define JLONG(context) ((jlong)((ptrdiff_t)(context)))

#define KEY_LENGTH_128 16
#define KEY_LENGTH_256 32
#define IV_LENGTH 16

#define ENCRYPT_MODE 1
#define DECRYPT_MODE 0

/** Currently only support AES/CTR/NoPadding. */
#define AES_CTR 0
#define NOPADDING 0
#define PKCSPADDING 1

#endif //ORG_APACHE_HADOOP_CRYPTO_H