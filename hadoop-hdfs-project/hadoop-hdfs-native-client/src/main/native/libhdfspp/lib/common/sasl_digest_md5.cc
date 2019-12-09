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
#include "sasl_authenticator.h"

#include "common/util.h"

#include <openssl/rand.h>
#include <openssl/md5.h>

#include <iomanip>
#include <map>
#include <sstream>

namespace hdfs {

static std::string QuoteString(const std::string &src);
static std::string GetMD5Digest(const std::string &src);
static std::string BinaryToHex(const std::string &src);

static const char kDigestUri[] = "hdfs/0";
static const size_t kMaxBufferSize = 65536;

DigestMD5Authenticator::DigestMD5Authenticator(const std::string &username,
                                               const std::string &password,
                                               bool mock_nonce)
    : username_(username), password_(password), nonce_count_(0),
      TEST_mock_cnonce_(mock_nonce) {}

Status DigestMD5Authenticator::EvaluateResponse(const std::string &payload,
                                                std::string *result) {
  Status status = ParseFirstChallenge(payload);
  if (status.ok()) {
    status = GenerateFirstResponse(result);
  }
  return status;
}

size_t DigestMD5Authenticator::NextToken(const std::string &payload, size_t off,
                                         std::string *tok) {
  tok->clear();
  if (off >= payload.size()) {
    return std::string::npos;
  }

  char c = payload[off];
  if (c == '=' || c == ',') {
    *tok = c;
    return off + 1;
  }

  int quote_count = 0;
  for (; off < payload.size(); ++off) {
    char c = payload[off];
    if (c == '"') {
      ++quote_count;
      if (quote_count == 2) {
        return off + 1;
      }
      continue;
    }

    if (c == '=') {
      if (quote_count) {
        tok->append(&c, 1);
      } else {
        break;
      }
    } else if (('0' <= c && c <= '9') || ('a' <= c && c <= 'z') ||
               ('A' <= c && c <= 'Z') || c == '+' || c == '/' || c == '-' ||
               c == '_' || c == '@') {
      tok->append(&c, 1);
    } else {
      break;
    }
  }
  return off;
}

void DigestMD5Authenticator::GenerateCNonce() {
  if (!TEST_mock_cnonce_) {
    char buf[8] = {0,};
    RAND_pseudo_bytes(reinterpret_cast<unsigned char *>(buf), sizeof(buf));
    cnonce_ = Base64Encode(std::string(buf, sizeof(buf)));
  }
}

Status DigestMD5Authenticator::ParseFirstChallenge(const std::string &payload) {
  std::map<std::string, std::string> props;
  std::string token;
  enum {
    kStateLVal,
    kStateEqual,
    kStateRVal,
    kStateCommaOrEnd,
  };

  int state = kStateLVal;

  std::string lval, rval;
  size_t off = 0;
  while (true) {
    off = NextToken(payload, off, &token);
    if (off == std::string::npos) {
      break;
    }

    switch (state) {
    case kStateLVal:
      lval = token;
      state = kStateEqual;
      break;
    case kStateEqual:
      state = kStateRVal;
      break;
    case kStateRVal:
      rval = token;
      props[lval] = rval;
      state = kStateCommaOrEnd;
      break;
    case kStateCommaOrEnd:
      state = kStateLVal;
      break;
    }
  }

  if (props["algorithm"] != "md5-sess" || props["charset"] != "utf-8" ||
      props.find("nonce") == props.end()) {
    return Status::Error("Invalid challenge");
  }
  realm_ = props["realm"];
  nonce_ = props["nonce"];
  qop_ = props["qop"];
  return Status::OK();
}

Status DigestMD5Authenticator::GenerateFirstResponse(std::string *result) {
  // TODO: Support auth-int and auth-conf
  // Handle cipher
  if (qop_ != "auth") {
    return Status::Unimplemented();
  }

  std::stringstream ss;
  GenerateCNonce();
  ss << "charset=utf-8,username=\"" << QuoteString(username_) << "\""
     << ",authzid=\"" << QuoteString(username_) << "\""
     << ",nonce=\"" << QuoteString(nonce_) << "\""
     << ",digest-uri=\"" << kDigestUri << "\""
     << ",maxbuf=" << kMaxBufferSize << ",cnonce=\"" << cnonce_ << "\"";

  if (realm_.size()) {
    ss << ",realm=\"" << QuoteString(realm_) << "\"";
  }

  ss << ",nc=" << std::hex << std::setw(8) << std::setfill('0')
     << ++nonce_count_;
  std::string response_value;
  GenerateResponseValue(&response_value);
  ss << ",response=" << response_value;
  *result = ss.str();
  return result->size() > 4096 ? Status::Error("Response too big")
                               : Status::OK();
}

/**
 * Generate the response value specified in S 2.1.2.1 in RFC2831.
 **/
Status
DigestMD5Authenticator::GenerateResponseValue(std::string *response_value) {
  std::stringstream begin_a1, a1_ss;
  std::string a1, a2;

  if (qop_ == "auth") {
    a2 = std::string("AUTHENTICATE:") + kDigestUri;
  } else {
    a2 = std::string("AUTHENTICATE:") + kDigestUri +
         ":00000000000000000000000000000000";
  }

  begin_a1 << username_ << ":" << realm_ << ":" << password_;
  a1_ss << GetMD5Digest(begin_a1.str()) << ":" << nonce_ << ":" << cnonce_
        << ":" << username_;

  std::stringstream combine_ss;
  combine_ss << BinaryToHex(GetMD5Digest(a1_ss.str())) << ":" << nonce_ << ":"
             << std::hex << std::setw(8) << std::setfill('0') << nonce_count_
             << ":" << cnonce_ << ":" << qop_ << ":"
             << BinaryToHex(GetMD5Digest(a2));
  *response_value = BinaryToHex(GetMD5Digest(combine_ss.str()));
  return Status::OK();
}

static std::string QuoteString(const std::string &src) {
  std::string dst;
  dst.resize(2 * src.size());
  size_t j = 0;
  for (size_t i = 0; i < src.size(); ++i) {
    if (src[i] == '"') {
      dst[j++] = '\\';
    }
    dst[j++] = src[i];
  }
  dst.resize(j);
  return dst;
}

static std::string GetMD5Digest(const std::string &src) {
  MD5_CTX ctx;
  unsigned long long res[2];
  MD5_Init(&ctx);
  MD5_Update(&ctx, src.c_str(), src.size());
  MD5_Final(reinterpret_cast<unsigned char *>(res), &ctx);
  return std::string(reinterpret_cast<char *>(res), sizeof(res));
}

static std::string BinaryToHex(const std::string &src) {
  std::stringstream ss;
  ss << std::hex << std::setfill('0');
  for (size_t i = 0; i < src.size(); ++i) {
    unsigned c = (unsigned)(static_cast<unsigned char>(src[i]));
    ss << std::setw(2) << c;
  }
  return ss.str();
}
}
