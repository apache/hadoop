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
 
#include "org_apache_hadoop_crypto.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
 
#include "org_apache_hadoop_crypto_OpensslCipher.h"

/*
   # OpenSSL ABI Symbols

   Available on all OpenSSL versions:

   | Function                       | 1.0 | 1.1 | 3.0 |
   |--------------------------------|-----|-----|-----|
   | EVP_CIPHER_CTX_new             | YES | YES | YES |
   | EVP_CIPHER_CTX_free            | YES | YES | YES |
   | EVP_CIPHER_CTX_set_padding     | YES | YES | YES |
   | EVP_CIPHER_CTX_test_flags      | YES | YES | YES |
   | EVP_CipherInit_ex              | YES | YES | YES |
   | EVP_CipherUpdate               | YES | YES | YES |
   | EVP_CipherFinal_ex             | YES | YES | YES |
   | ENGINE_by_id                   | YES | YES | YES |
   | ENGINE_free                    | YES | YES | YES |
   | EVP_aes_256_ctr                | YES | YES | YES |
   | EVP_aes_128_ctr                | YES | YES | YES |

   Available on old versions:

   | Function                       | 1.0 | 1.1 | 3.0 |
   |--------------------------------|-----|-----|-----|
   | EVP_CIPHER_CTX_cleanup         | YES | --- | --- |
   | EVP_CIPHER_CTX_init            | YES | --- | --- |
   | EVP_CIPHER_CTX_block_size      | YES | YES | --- |
   | EVP_CIPHER_CTX_encrypting      | --- | YES | --- |

   Available on new versions:

   | Function                       | 1.0 | 1.1 | 3.0 |
   |--------------------------------|-----|-----|-----|
   | OPENSSL_init_crypto            | --- | YES | YES |
   | EVP_CIPHER_CTX_reset           | --- | YES | YES |
   | EVP_CIPHER_CTX_get_block_size  | --- | --- | YES |
   | EVP_CIPHER_CTX_is_encrypting   | --- | --- | YES |

   Optionally available on new versions:

   | Function                       | 1.0 | 1.1 | 3.0 |
   |--------------------------------|-----|-----|-----|
   | EVP_sm4_ctr                    | --- | opt | opt |

   Name changes:

   | < 3.0 name                 | >= 3.0 name                    |
   |----------------------------|--------------------------------|
   | EVP_CIPHER_CTX_block_size  | EVP_CIPHER_CTX_get_block_size  |
   | EVP_CIPHER_CTX_encrypting  | EVP_CIPHER_CTX_is_encrypting   |
 */

#ifdef UNIX
static EVP_CIPHER_CTX * (*dlsym_EVP_CIPHER_CTX_new)(void);
static void (*dlsym_EVP_CIPHER_CTX_free)(EVP_CIPHER_CTX *);
#if OPENSSL_API_COMPAT < 0x10100000L && OPENSSL_VERSION_NUMBER >= 0x10100000L
static int (*dlsym_EVP_CIPHER_CTX_reset)(EVP_CIPHER_CTX *);
#else
static int (*dlsym_EVP_CIPHER_CTX_cleanup)(EVP_CIPHER_CTX *);
static void (*dlsym_EVP_CIPHER_CTX_init)(EVP_CIPHER_CTX *);
#endif
static int (*dlsym_EVP_CIPHER_CTX_set_padding)(EVP_CIPHER_CTX *, int);
static int (*dlsym_EVP_CIPHER_CTX_test_flags)(const EVP_CIPHER_CTX *, int);
static int (*dlsym_EVP_CIPHER_CTX_block_size)(const EVP_CIPHER_CTX *);
#if OPENSSL_VERSION_NUMBER >= 0x10100000L
static int (*dlsym_EVP_CIPHER_CTX_encrypting)(const EVP_CIPHER_CTX *);
#endif
static int (*dlsym_EVP_CipherInit_ex)(EVP_CIPHER_CTX *, const EVP_CIPHER *,  \
           ENGINE *, const unsigned char *, const unsigned char *, int);
static int (*dlsym_EVP_CipherUpdate)(EVP_CIPHER_CTX *, unsigned char *,  \
           int *, const unsigned char *, int);
static int (*dlsym_EVP_CipherFinal_ex)(EVP_CIPHER_CTX *, unsigned char *, int *);
static EVP_CIPHER * (*dlsym_EVP_aes_256_ctr)(void);
static EVP_CIPHER * (*dlsym_EVP_aes_128_ctr)(void);
#if OPENSSL_VERSION_NUMBER >= 0x10100001L
static EVP_CIPHER * (*dlsym_EVP_sm4_ctr)(void);
static int (*dlsym_OPENSSL_init_crypto)(uint64_t opts, \
            const OPENSSL_INIT_SETTINGS *settings);
static ENGINE * (*dlsym_ENGINE_by_id)(const char *id);
static int (*dlsym_ENGINE_free)(ENGINE *);
#endif
static void *openssl;
#endif

#ifdef WINDOWS
typedef EVP_CIPHER_CTX * (__cdecl *__dlsym_EVP_CIPHER_CTX_new)(void);
typedef void (__cdecl *__dlsym_EVP_CIPHER_CTX_free)(EVP_CIPHER_CTX *);
typedef int (__cdecl *__dlsym_EVP_CIPHER_CTX_cleanup)(EVP_CIPHER_CTX *);
typedef void (__cdecl *__dlsym_EVP_CIPHER_CTX_init)(EVP_CIPHER_CTX *);
typedef int (__cdecl *__dlsym_EVP_CIPHER_CTX_set_padding)(EVP_CIPHER_CTX *, int);
typedef int (__cdecl *__dlsym_EVP_CIPHER_CTX_test_flags)(const EVP_CIPHER_CTX *, int);
typedef int (__cdecl *__dlsym_EVP_CIPHER_CTX_block_size)(const EVP_CIPHER_CTX *);
#if OPENSSL_VERSION_NUMBER >= 0x10100000L
typedef int (__cdecl *__dlsym_EVP_CIPHER_CTX_encrypting)(const EVP_CIPHER_CTX *);
#endif
typedef int (__cdecl *__dlsym_EVP_CipherInit_ex)(EVP_CIPHER_CTX *,  \
             const EVP_CIPHER *, ENGINE *, const unsigned char *,  \
             const unsigned char *, int);
typedef int (__cdecl *__dlsym_EVP_CipherUpdate)(EVP_CIPHER_CTX *,  \
             unsigned char *, int *, const unsigned char *, int);
typedef int (__cdecl *__dlsym_EVP_CipherFinal_ex)(EVP_CIPHER_CTX *,  \
             unsigned char *, int *);
typedef EVP_CIPHER * (__cdecl *__dlsym_EVP_aes_256_ctr)(void);
typedef EVP_CIPHER * (__cdecl *__dlsym_EVP_aes_128_ctr)(void);
static __dlsym_EVP_CIPHER_CTX_new dlsym_EVP_CIPHER_CTX_new;
static __dlsym_EVP_CIPHER_CTX_free dlsym_EVP_CIPHER_CTX_free;
static __dlsym_EVP_CIPHER_CTX_cleanup dlsym_EVP_CIPHER_CTX_cleanup;
static __dlsym_EVP_CIPHER_CTX_init dlsym_EVP_CIPHER_CTX_init;
static __dlsym_EVP_CIPHER_CTX_set_padding dlsym_EVP_CIPHER_CTX_set_padding;
static __dlsym_EVP_CIPHER_CTX_test_flags dlsym_EVP_CIPHER_CTX_test_flags;
static __dlsym_EVP_CIPHER_CTX_block_size dlsym_EVP_CIPHER_CTX_block_size;
#if OPENSSL_VERSION_NUMBER >= 0x10100000L
static __dlsym_EVP_CIPHER_CTX_encrypting dlsym_EVP_CIPHER_CTX_encrypting;
#endif
static __dlsym_EVP_CipherInit_ex dlsym_EVP_CipherInit_ex;
static __dlsym_EVP_CipherUpdate dlsym_EVP_CipherUpdate;
static __dlsym_EVP_CipherFinal_ex dlsym_EVP_CipherFinal_ex;
static __dlsym_EVP_aes_256_ctr dlsym_EVP_aes_256_ctr;
static __dlsym_EVP_aes_128_ctr dlsym_EVP_aes_128_ctr;
#if OPENSSL_VERSION_NUMBER >= 0x10101001L
typedef EVP_CIPHER * (__cdecl *__dlsym_EVP_sm4_ctr)(void);
typedef int (__cdecl *__dlsym_OPENSSL_init_crypto)(uint64_t opts, \
             const OPENSSL_INIT_SETTINGS *settings);
typedef ENGINE * (__cdecl *__dlsym_ENGINE_by_id)(const char *id);
typedef int (__cdecl *__dlsym_ENGINE_free)(ENGINE *e);

static __dlsym_EVP_sm4_ctr dlsym_EVP_sm4_ctr;
static __dlsym_OPENSSL_init_crypto dlsym_OPENSSL_init_crypto;
static __dlsym_ENGINE_by_id dlsym_ENGINE_by_id;
static __dlsym_ENGINE_free dlsym_ENGINE_free;
#endif
static HMODULE openssl;
#endif

// names changed in OpenSSL 3 ABI - see History section in EVP_EncryptInit(3)
#if OPENSSL_VERSION_NUMBER >= 0x30000000L
#define CIPHER_CTX_BLOCK_SIZE "EVP_CIPHER_CTX_get_block_size"
#define CIPHER_CTX_ENCRYPTING "EVP_CIPHER_CTX_is_encrypting"
#else
#define CIPHER_CTX_BLOCK_SIZE "EVP_CIPHER_CTX_block_size"
#define CIPHER_CTX_ENCRYPTING "EVP_CIPHER_CTX_encrypting"
#endif /* OPENSSL_VERSION_NUMBER >= 0x30000000L */

static void loadAesCtr(JNIEnv *env)
{
#ifdef UNIX
  LOAD_DYNAMIC_SYMBOL(dlsym_EVP_aes_256_ctr, env, openssl, "EVP_aes_256_ctr");
  LOAD_DYNAMIC_SYMBOL(dlsym_EVP_aes_128_ctr, env, openssl, "EVP_aes_128_ctr");
#endif

#ifdef WINDOWS
  LOAD_DYNAMIC_SYMBOL(__dlsym_EVP_aes_256_ctr, dlsym_EVP_aes_256_ctr,  \
                      env, openssl, "EVP_aes_256_ctr");
  LOAD_DYNAMIC_SYMBOL(__dlsym_EVP_aes_128_ctr, dlsym_EVP_aes_128_ctr,  \
                      env, openssl, "EVP_aes_128_ctr");
#endif
}

static void loadSm4Ctr(JNIEnv *env)
{
#ifdef UNIX
#if OPENSSL_VERSION_NUMBER >= 0x10101001L
   LOAD_DYNAMIC_SYMBOL(dlsym_EVP_sm4_ctr, env, openssl, "EVP_sm4_ctr");
#endif
#endif
}

JNIEXPORT void JNICALL Java_org_apache_hadoop_crypto_OpensslCipher_initIDs
    (JNIEnv *env, jclass clazz)
{
  char msg[1000];
#ifdef UNIX
  openssl = dlopen(HADOOP_OPENSSL_LIBRARY, RTLD_LAZY | RTLD_GLOBAL);
#endif

#ifdef WINDOWS
  openssl = LoadLibrary(HADOOP_OPENSSL_LIBRARY);
#endif

  if (!openssl) {
    snprintf(msg, sizeof(msg), "Cannot load %s (%s)!", HADOOP_OPENSSL_LIBRARY,  \
        dlerror());
    THROW(env, "java/lang/UnsatisfiedLinkError", msg);
    return;
  }

#ifdef UNIX
  dlerror();  // Clear any existing error
  LOAD_DYNAMIC_SYMBOL(dlsym_EVP_CIPHER_CTX_new, env, openssl,  \
                      "EVP_CIPHER_CTX_new");
  LOAD_DYNAMIC_SYMBOL(dlsym_EVP_CIPHER_CTX_free, env, openssl,  \
                      "EVP_CIPHER_CTX_free");
#if OPENSSL_API_COMPAT < 0x10100000L && OPENSSL_VERSION_NUMBER >= 0x10100000L
  LOAD_DYNAMIC_SYMBOL(dlsym_EVP_CIPHER_CTX_reset, env, openssl,  \
                      "EVP_CIPHER_CTX_reset");
#else
  LOAD_DYNAMIC_SYMBOL(dlsym_EVP_CIPHER_CTX_cleanup, env, openssl,  \
                      "EVP_CIPHER_CTX_cleanup");
  LOAD_DYNAMIC_SYMBOL(dlsym_EVP_CIPHER_CTX_init, env, openssl,  \
                      "EVP_CIPHER_CTX_init");
#endif

  LOAD_DYNAMIC_SYMBOL(dlsym_EVP_CIPHER_CTX_set_padding, env, openssl,  \
                      "EVP_CIPHER_CTX_set_padding");
  LOAD_DYNAMIC_SYMBOL(dlsym_EVP_CIPHER_CTX_test_flags, env, openssl,  \
                      "EVP_CIPHER_CTX_test_flags");
  LOAD_DYNAMIC_SYMBOL(dlsym_EVP_CIPHER_CTX_block_size, env, openssl,  \
                      CIPHER_CTX_BLOCK_SIZE);
#if OPENSSL_VERSION_NUMBER >= 0x10100000L
  LOAD_DYNAMIC_SYMBOL(dlsym_EVP_CIPHER_CTX_encrypting, env, openssl,  \
                      CIPHER_CTX_ENCRYPTING);
#endif
  LOAD_DYNAMIC_SYMBOL(dlsym_EVP_CipherInit_ex, env, openssl,  \
                      "EVP_CipherInit_ex");
  LOAD_DYNAMIC_SYMBOL(dlsym_EVP_CipherUpdate, env, openssl,  \
                      "EVP_CipherUpdate");
  LOAD_DYNAMIC_SYMBOL(dlsym_EVP_CipherFinal_ex, env, openssl,  \
                      "EVP_CipherFinal_ex");
#if OPENSSL_VERSION_NUMBER >= 0x10101001L
  LOAD_DYNAMIC_SYMBOL(dlsym_OPENSSL_init_crypto, env, openssl,  \
                      "OPENSSL_init_crypto");
  LOAD_DYNAMIC_SYMBOL(dlsym_ENGINE_by_id, env, openssl,  \
                      "ENGINE_by_id");
  LOAD_DYNAMIC_SYMBOL(dlsym_ENGINE_free, env, openssl,  \
                      "ENGINE_free");
#endif
#endif

#ifdef WINDOWS
  LOAD_DYNAMIC_SYMBOL(__dlsym_EVP_CIPHER_CTX_new, dlsym_EVP_CIPHER_CTX_new,  \
                      env, openssl, "EVP_CIPHER_CTX_new");
  LOAD_DYNAMIC_SYMBOL(__dlsym_EVP_CIPHER_CTX_free, dlsym_EVP_CIPHER_CTX_free,  \
                      env, openssl, "EVP_CIPHER_CTX_free");
  LOAD_DYNAMIC_SYMBOL(__dlsym_EVP_CIPHER_CTX_cleanup,  \
                      dlsym_EVP_CIPHER_CTX_cleanup, env, 
                      openssl, "EVP_CIPHER_CTX_cleanup");
  LOAD_DYNAMIC_SYMBOL(__dlsym_EVP_CIPHER_CTX_init, dlsym_EVP_CIPHER_CTX_init,  \
                      env, openssl, "EVP_CIPHER_CTX_init");
  LOAD_DYNAMIC_SYMBOL(__dlsym_EVP_CIPHER_CTX_set_padding,  \
                      dlsym_EVP_CIPHER_CTX_set_padding, env,  \
                      openssl, "EVP_CIPHER_CTX_set_padding");
  LOAD_DYNAMIC_SYMBOL(__dlsym_EVP_CIPHER_CTX_test_flags,  \
                      dlsym_EVP_CIPHER_CTX_test_flags, env,  \
                      openssl, "EVP_CIPHER_CTX_test_flags");
  LOAD_DYNAMIC_SYMBOL(__dlsym_EVP_CIPHER_CTX_block_size,  \
                      dlsym_EVP_CIPHER_CTX_block_size, env,  \
                      openssl, CIPHER_CTX_BLOCK_SIZE);
#if OPENSSL_VERSION_NUMBER >= 0x10100000L
  LOAD_DYNAMIC_SYMBOL(__dlsym_EVP_CIPHER_CTX_encrypting,  \
                      dlsym_EVP_CIPHER_CTX_encrypting, env,  \
                      openssl, CIPHER_CTX_ENCRYPTING);
#endif
  LOAD_DYNAMIC_SYMBOL(__dlsym_EVP_CipherInit_ex, dlsym_EVP_CipherInit_ex,  \
                      env, openssl, "EVP_CipherInit_ex");
  LOAD_DYNAMIC_SYMBOL(__dlsym_EVP_CipherUpdate, dlsym_EVP_CipherUpdate,  \
                      env, openssl, "EVP_CipherUpdate");
  LOAD_DYNAMIC_SYMBOL(__dlsym_EVP_CipherFinal_ex, dlsym_EVP_CipherFinal_ex,  \
                      env, openssl, "EVP_CipherFinal_ex");
#if OPENSSL_VERSION_NUMBER >= 0x10101001L
  LOAD_DYNAMIC_SYMBOL(__dlsym_OPENSSL_init_crypto, dlsym_OPENSSL_init_crypto,  \
                      env, openssl, "OPENSSL_init_crypto");
  LOAD_DYNAMIC_SYMBOL(__dlsym_ENGINE_by_id, dlsym_ENGINE_by_id,  \
                      env, openssl, "ENGINE_by_id");
  LOAD_DYNAMIC_SYMBOL(__dlsym_ENGINE_free, dlsym_ENGINE_free,  \
                      env, openssl, "ENGINE_by_free");
#endif
#endif

  loadAesCtr(env);
#if !defined(OPENSSL_NO_SM4)
  loadSm4Ctr(env);
#endif

#if OPENSSL_VERSION_NUMBER >= 0x10101001L
  int ret = dlsym_OPENSSL_init_crypto(OPENSSL_INIT_LOAD_CONFIG, NULL);
  if(!ret) {
    THROW(env, "java/lang/UnsatisfiedLinkError", \
        "Openssl init crypto failed");
    return;
  }
#endif
  jthrowable jthr = (*env)->ExceptionOccurred(env);
  if (jthr) {
    (*env)->DeleteLocalRef(env, jthr);
    THROW(env, "java/lang/UnsatisfiedLinkError",  \
        "Cannot find AES-CTR support, is your version of OpenSSL new enough?");
    return;
  }
}

JNIEXPORT jlong JNICALL Java_org_apache_hadoop_crypto_OpensslCipher_initContext
    (JNIEnv *env, jclass clazz, jint alg, jint padding)
{
  if (alg != AES_CTR && alg != SM4_CTR) {
    THROW(env, "java/security/NoSuchAlgorithmException", NULL);
    return (jlong)0;
  }
  if (padding != NOPADDING) {
    THROW(env, "javax/crypto/NoSuchPaddingException", NULL);
    return (jlong)0;
  }
  
  if (alg == AES_CTR && (dlsym_EVP_aes_256_ctr == NULL || dlsym_EVP_aes_128_ctr == NULL)) {
    THROW(env, "java/security/NoSuchAlgorithmException",  \
        "Doesn't support AES CTR.");
    return (jlong)0;
  }

  if (alg == SM4_CTR) {
    int ret = 0;
#if OPENSSL_VERSION_NUMBER >= 0x10101001L
    if (dlsym_EVP_sm4_ctr == NULL) {
      ret = 1;
    }
#else
    ret = 1;
#endif
    if (ret) {
      THROW(env, "java/security/NoSuchAlgorithmException",  \
              "Doesn't support SM4 CTR.");
      return (jlong)0;
    }
  }
  
  // Create and initialize a EVP_CIPHER_CTX
  EVP_CIPHER_CTX *context = dlsym_EVP_CIPHER_CTX_new();
  if (!context) {
    THROW(env, "java/lang/OutOfMemoryError", NULL);
    return (jlong)0;
  }
   
  return JLONG(context);
}

JNIEXPORT jlong JNICALL Java_org_apache_hadoop_crypto_OpensslCipher_initEngine
    (JNIEnv *env, jclass clazz, jstring engineId)
{
  ENGINE *e = NULL;

#if OPENSSL_VERSION_NUMBER >= 0x10101001L
  if (engineId != NULL) {
    const char *id = (*env)->GetStringUTFChars(env, engineId, NULL);
    if (id != NULL) {
      e = dlsym_ENGINE_by_id(id);
      (*env)->ReleaseStringUTFChars(env, engineId, id);
    }
  }
#endif

  if (e == NULL) {
    return (jlong)0;
  } else {
    return JLONG(e);
  }
}

// Only supports AES-CTR & SM4-CTR currently
static EVP_CIPHER * getEvpCipher(int alg, int keyLen)
{
  EVP_CIPHER *cipher = NULL;
  if (alg == AES_CTR) {
    if (keyLen == KEY_LENGTH_256) {
      cipher = dlsym_EVP_aes_256_ctr();
    } else if (keyLen == KEY_LENGTH_128) {
      cipher = dlsym_EVP_aes_128_ctr();
    }
  } else if (alg == SM4_CTR) {
    if (keyLen == KEY_LENGTH_128) {
#if OPENSSL_VERSION_NUMBER >= 0x10101001L
      cipher = dlsym_EVP_sm4_ctr();
#endif
    }
  }
  return cipher;
}

JNIEXPORT jlong JNICALL Java_org_apache_hadoop_crypto_OpensslCipher_init
    (JNIEnv *env, jobject object, jlong ctx, jint mode, jint alg, jint padding, 
    jbyteArray key, jbyteArray iv, jlong engine)
{
  int jKeyLen = (*env)->GetArrayLength(env, key);
  int jIvLen = (*env)->GetArrayLength(env, iv);
  if (jKeyLen != KEY_LENGTH_128 && jKeyLen != KEY_LENGTH_256) {
    THROW(env, "java/lang/IllegalArgumentException", "Invalid key length.");
    return (jlong)0;
  }
  if (jIvLen != IV_LENGTH) {
    THROW(env, "java/lang/IllegalArgumentException", "Invalid iv length.");
    return (jlong)0;
  }
  
  EVP_CIPHER_CTX *context = CONTEXT(ctx);
  if (context == 0) {
    // Create and initialize a EVP_CIPHER_CTX
    context = dlsym_EVP_CIPHER_CTX_new();
    if (!context) {
      THROW(env, "java/lang/OutOfMemoryError", NULL);
      return (jlong)0;
    }
  }
  
  jbyte *jKey = (*env)->GetByteArrayElements(env, key, NULL);
  if (jKey == NULL) {
    THROW(env, "java/lang/InternalError", "Cannot get bytes array for key.");
    return (jlong)0;
  }
  jbyte *jIv = (*env)->GetByteArrayElements(env, iv, NULL);
  if (jIv == NULL) {
    (*env)->ReleaseByteArrayElements(env, key, jKey, 0);
    THROW(env, "java/lang/InternalError", "Cannot get bytes array for iv.");
    return (jlong)0;
  }

  ENGINE *e = LONG_TO_ENGINE(engine);
  int rc = dlsym_EVP_CipherInit_ex(context, getEvpCipher(alg, jKeyLen),  \
      e, (unsigned char *)jKey, (unsigned char *)jIv, mode == ENCRYPT_MODE);
  (*env)->ReleaseByteArrayElements(env, key, jKey, 0);
  (*env)->ReleaseByteArrayElements(env, iv, jIv, 0);
  if (rc == 0) {
#if OPENSSL_API_COMPAT < 0x10100000L && OPENSSL_VERSION_NUMBER >= 0x10100000L
    dlsym_EVP_CIPHER_CTX_reset(context);
#else
    dlsym_EVP_CIPHER_CTX_cleanup(context);
#endif
    THROW(env, "java/lang/InternalError", "Error in EVP_CipherInit_ex.");
    return (jlong)0;
  }
  
  if (padding == NOPADDING) {
    dlsym_EVP_CIPHER_CTX_set_padding(context, 0);
  }
  
  return JLONG(context);
}

// https://www.openssl.org/docs/crypto/EVP_EncryptInit.html
static int check_update_max_output_len(EVP_CIPHER_CTX *context, int input_len, 
    int max_output_len)
{
  if (  dlsym_EVP_CIPHER_CTX_test_flags(context, EVP_CIPH_NO_PADDING) ) {
    if (max_output_len >= input_len) {
      return 1;
    }
    return 0;
  } else {
    int b = dlsym_EVP_CIPHER_CTX_block_size(context);
#if OPENSSL_VERSION_NUMBER < 0x10100000L
    if (context->encrypt) {
#else
    if (dlsym_EVP_CIPHER_CTX_encrypting(context)) {
#endif
      if (max_output_len >= input_len + b - 1) {
        return 1;
      }
    } else {
      if (max_output_len >= input_len + b) {
        return 1;
      }
    }
    
    return 0;
  }
}

JNIEXPORT jint JNICALL Java_org_apache_hadoop_crypto_OpensslCipher_update
    (JNIEnv *env, jobject object, jlong ctx, jobject input, jint input_offset, 
    jint input_len, jobject output, jint output_offset, jint max_output_len)
{
  EVP_CIPHER_CTX *context = CONTEXT(ctx);
  if (!check_update_max_output_len(context, input_len, max_output_len)) {
    THROW(env, "javax/crypto/ShortBufferException",  \
        "Output buffer is not sufficient.");
    return 0;
  }
  unsigned char *input_bytes = (*env)->GetDirectBufferAddress(env, input);
  unsigned char *output_bytes = (*env)->GetDirectBufferAddress(env, output);
  if (input_bytes == NULL || output_bytes == NULL) {
    THROW(env, "java/lang/InternalError", "Cannot get buffer address.");
    return 0;
  }
  input_bytes = input_bytes + input_offset;
  output_bytes = output_bytes + output_offset;
  
  int output_len = 0;
  if (!dlsym_EVP_CipherUpdate(context, output_bytes, &output_len,  \
      input_bytes, input_len)) {
#if OPENSSL_API_COMPAT < 0x10100000L && OPENSSL_VERSION_NUMBER >= 0x10100000L
    dlsym_EVP_CIPHER_CTX_reset(context);
#else
    dlsym_EVP_CIPHER_CTX_cleanup(context);
#endif
    THROW(env, "java/lang/InternalError", "Error in EVP_CipherUpdate.");
    return 0;
  }
  return output_len;
}

// https://www.openssl.org/docs/crypto/EVP_EncryptInit.html
static int check_doFinal_max_output_len(EVP_CIPHER_CTX *context, 
    int max_output_len)
{
  if (  dlsym_EVP_CIPHER_CTX_test_flags(context, EVP_CIPH_NO_PADDING) ) {
    return 1;
  } else {
    int b = dlsym_EVP_CIPHER_CTX_block_size(context);
    if (max_output_len >= b) {
      return 1;
    }
    
    return 0;
  }
}

JNIEXPORT jint JNICALL Java_org_apache_hadoop_crypto_OpensslCipher_doFinal
    (JNIEnv *env, jobject object, jlong ctx, jobject output, jint offset, 
    jint max_output_len)
{
  EVP_CIPHER_CTX *context = CONTEXT(ctx);
  if (!check_doFinal_max_output_len(context, max_output_len)) {
    THROW(env, "javax/crypto/ShortBufferException",  \
        "Output buffer is not sufficient.");
    return 0;
  }
  unsigned char *output_bytes = (*env)->GetDirectBufferAddress(env, output);
  if (output_bytes == NULL) {
    THROW(env, "java/lang/InternalError", "Cannot get buffer address.");
    return 0;
  }
  output_bytes = output_bytes + offset;
  
  int output_len = 0;
  if (!dlsym_EVP_CipherFinal_ex(context, output_bytes, &output_len)) {
#if OPENSSL_API_COMPAT < 0x10100000L && OPENSSL_VERSION_NUMBER >= 0x10100000L
    dlsym_EVP_CIPHER_CTX_reset(context);
#else
    dlsym_EVP_CIPHER_CTX_cleanup(context);
#endif
    THROW(env, "java/lang/InternalError", "Error in EVP_CipherFinal_ex.");
    return 0;
  }
  return output_len;
}

JNIEXPORT void JNICALL Java_org_apache_hadoop_crypto_OpensslCipher_clean
    (JNIEnv *env, jobject object, jlong ctx, jlong engine)
{
  EVP_CIPHER_CTX *context = CONTEXT(ctx);
  if (context) {
    dlsym_EVP_CIPHER_CTX_free(context);
  }

  ENGINE *e = LONG_TO_ENGINE(engine);
  if (e) {
    dlsym_ENGINE_free(e);
  }
}

JNIEXPORT jstring JNICALL Java_org_apache_hadoop_crypto_OpensslCipher_getLibraryName
    (JNIEnv *env, jclass clazz) 
{
#ifdef UNIX
#if OPENSSL_API_COMPAT < 0x10100000L && OPENSSL_VERSION_NUMBER >= 0x10100000L
  if (dlsym_EVP_CIPHER_CTX_reset) {
    Dl_info dl_info;
    if(dladdr(
        dlsym_EVP_CIPHER_CTX_reset,
        &dl_info)) {
      return (*env)->NewStringUTF(env, dl_info.dli_fname);
    }
  }
#else
  if (dlsym_EVP_CIPHER_CTX_init) {
    Dl_info dl_info;
    if(dladdr(
        dlsym_EVP_CIPHER_CTX_init,
        &dl_info)) {
      return (*env)->NewStringUTF(env, dl_info.dli_fname);
    }
  }
#endif

  return (*env)->NewStringUTF(env, HADOOP_OPENSSL_LIBRARY);
#endif

#ifdef WINDOWS
  LPWSTR filename = NULL;
  GetLibraryName(dlsym_EVP_CIPHER_CTX_init, &filename);
  if (filename != NULL) {
    return (*env)->NewString(env, filename, (jsize) wcslen(filename));
  } else {
    return (*env)->NewStringUTF(env, "Unavailable");
  }
#endif
}

JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_crypto_OpensslCipher_isSupportedSuite
    (JNIEnv *env, jclass clazz, jint alg, jint padding)
{
  if (padding != NOPADDING) {
    return JNI_FALSE;
  }

  if (alg == AES_CTR && (dlsym_EVP_aes_256_ctr != NULL && dlsym_EVP_aes_128_ctr != NULL)) {
    return JNI_TRUE;
  }

  if (alg == SM4_CTR) {
#if OPENSSL_VERSION_NUMBER >= 0x10101001L && !defined(OPENSSL_NO_SM4)
    if (dlsym_EVP_sm4_ctr != NULL) {
      return JNI_TRUE;
    }
#endif
  }
  return JNI_FALSE;
}
