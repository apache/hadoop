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

#include "hdfspp/locks.h"

#include <sys/types.h>
#include "sasl/sasl.h"
#include "sasl/saslutil.h"
#include <string.h>
#include <string>
#include <sstream>
#include <unistd.h>    // getpass() ( deprecated)

#include "common/logging.h"

#include       "sasl_engine.h"
#include "cyrus_sasl_engine.h"

namespace hdfs {

static Mutex *getSaslMutex() {
  return LockManager::getGssapiMutex();
}

// Forward decls of sasl callback functions
typedef int (*sasl_callback_ft)(void);
int get_name(void        *context,
             int          id,
             const char **result,
             unsigned    *len);

int getrealm(void *context,
             int   id,
             const char **availrealms,
             const char **result);

// This should be constructed once per process, and destroyed once per process

class CyrusPerProcessData
{
public:
  static Status Init(); // Can be called many times
private:
  CyrusPerProcessData();
  ~CyrusPerProcessData();
  Status init_status_;

  static CyrusPerProcessData & GetInstance();
};


/*****************************************************************************
 *              CYRUS UTILITY FUNCTIONS
 */

// Cyrus-specific error messages:
// errStr() is the non-method version, to
//          be called by utility routines.
std::string errStr( int rc) {
  switch (rc) {
    case SASL_NOTINIT:  /* -12 */ return "SASL library not initialized";
    case SASL_WRONGMECH:/* -11 */ return "mechanism doesn't support requested feature";
    case SASL_BADSERV:  /* -10 */ return "server failed mutual authentication step";
    case SASL_BADMAC:   /*  -9 */ return "integrity check failed";
    case SASL_TRYAGAIN: /*  -8 */ return "transient failure (e.g., weak key)";
    case SASL_BADPARAM: /*  -7 */ return "invalid parameter supplied";
    case SASL_NOTDONE:  /*  -6 */ return "can't request info until later in exchange";
    case SASL_BADPROT:  /*  -5 */ return "bad protocol / cancel";
    case SASL_NOMECH:   /*  -4 */ return "mechanism not supported";
    case SASL_BUFOVER:  /*  -3 */ return "overflowed buffer";
    case SASL_NOMEM:    /*  -2 */ return "memory shortage failure";
    case SASL_FAIL:     /*  -1 */ return "generic failure";
    case SASL_OK:       /*   0 */ return "successful result";
    case SASL_CONTINUE: /*   1 */ return "another step is needed in authentication";
    case SASL_INTERACT: /*   2 */ return "needs user interaction";
    default:                      return "unknown error";
  } // switch(rc)
} // errStr()

Status make_status(int rc) {
  if (rc != SASL_OK &&
      rc != SASL_CONTINUE &&
      rc != SASL_INTERACT) {
     return Status::AuthenticationFailed(errStr(rc).c_str());
  }
  return Status::OK();
}

// SaslError() method:  Use this when a method needs
//                   to update the engine's state.
Status CySaslEngine::SaslError( int rc) {
  Status status = make_status(rc);
  if (!status.ok())
      state_ = kErrorState;

  return status;
}


/*****************************************************************************
*                     Cyrus SASL ENGINE
*/

CySaslEngine::CySaslEngine() : SaslEngine(), conn_(nullptr)
{
  // Create an array of callbacks that embed a pointer to this
  //   so we can call methods of the engine
  per_connection_callbacks_ = {
    { SASL_CB_USER,     (sasl_callback_ft) & get_name, this}, // userid for authZ
    { SASL_CB_AUTHNAME, (sasl_callback_ft) & get_name, this}, // authid for authT
    { SASL_CB_GETREALM, (sasl_callback_ft) & getrealm, this}, // krb/gssapi realm
    //  { SASL_CB_PASS,        (sasl_callback_ft)&getsecret,  this
    { SASL_CB_LIST_END, (sasl_callback_ft) NULL, NULL}
  };
}

// Cleanup of last resort.  Call Finish to allow a safer check on disposal
CySaslEngine::~CySaslEngine()
{

  if (conn_) {
    try {
      LockGuard saslGuard(getSaslMutex());
      sasl_dispose( &conn_); // undo sasl_client_new()
    } catch (const LockFailure& e) {
      LOG_ERROR(kRPC, << "Unable to dispose of SASL context due to " << e.what());
    }
  }
} // destructor

// initialize some cyrus sasl context stuff:

Status CySaslEngine::InitCyrusSasl()
{
  int rc = SASL_OK;

  // set up some callbacks once per process:
  Status init_status = CyrusPerProcessData::Init();
  if (!init_status.ok())
    return init_status;

  // Initialize the sasl_li  brary with per-connection configuration:
  const char * fqdn = chosen_mech_.serverid.c_str();
  const char * proto = chosen_mech_.protocol.c_str();

  try {
    LockGuard saslGuard(getSaslMutex());
    rc = sasl_client_new(proto, fqdn, NULL, NULL, &per_connection_callbacks_[0], 0, &conn_);
    if (rc != SASL_OK) {
      return SaslError(rc);
    }
  } catch (const LockFailure& e) {
    return Status::MutexError("mutex that guards sasl_client_new unable to lock");
  }

  return Status::OK();
} // cysasl_new()

// start() method:  Ask the Sasl ibrary, "How do we
//                  ask the hdfs server for service?
std::pair<Status, std::string>
CySaslEngine::Start()
{
  int    rc;
  Status status;

  if (state_ != kUnstarted)
    LOG_WARN(kRPC, << "CySaslEngine::start() when state is " << state_);

  status = InitCyrusSasl();

  if ( !status.ok()) {
    state_ = kErrorState;
    return std::make_pair( status, "");
  }

  sasl_interact_t * client_interact = NULL;
  char            * buf;
  unsigned int      buflen;
  const char      * chosen_mech;
  std::string       token;

  try {
    LockGuard saslGuard(getSaslMutex());
    rc = sasl_client_start(conn_, chosen_mech_.mechanism.c_str(), &client_interact,
              (const char **) &buf, &buflen, &chosen_mech);
  } catch (const LockFailure& e) {
    state_ = kFailure;
    return std::make_pair( Status::MutexError("mutex that guards sasl_client_new unable to lock"), "" );
  }


  switch (rc) {
  case SASL_OK:        state_ = kSuccess;
                       break;
  case SASL_CONTINUE:  state_ = kWaitingForData;
                       break;
  default:             state_ = kFailure;
                       return std::make_pair( SaslError(rc), "");
                       break;
  } // switch( rc)

  // Cyrus will free this buffer when the connection is shut down
  token = std::string( buf, buflen);
  return std::make_pair( Status::OK(), token);

} // start() method

std::pair<Status, std::string> CySaslEngine::Step(const std::string data)
{
  char            * output = NULL;
  unsigned int      outlen = 0;
  sasl_interact_t * client_interact = NULL;

  if (state_ != kWaitingForData)
    LOG_WARN(kRPC, << "CySaslEngine::step when state is " << state_);

  int rc = 0;
  try {
    LockGuard saslGuard(getSaslMutex());
    rc = sasl_client_step(conn_, data.c_str(), data.size(), &client_interact,
                     (const char **) &output, &outlen);
  } catch (const LockFailure& e) {
    state_ = kFailure;
    return std::make_pair( Status::MutexError("mutex that guards sasl_client_new unable to lock"), "" );
  }
  // right now, state_ == kWaitingForData,
  // so update  state_, to reflect _step()'s result:
  switch (rc) {
  case SASL_OK:        state_ = kSuccess;        break;
  case SASL_CONTINUE:  state_ = kWaitingForData; break;
  default:             state_ = kFailure;
               return std::make_pair(SaslError(rc), "");
               break;
  } // switch( rc)
  return std::make_pair(Status::OK(), std::string( output,outlen));
} // step() method

Status CySaslEngine::Finish()
{
  if (state_ != kSuccess && state_ != kFailure && state_ != kErrorState )
    LOG_WARN(kRPC, << "CySaslEngine::finish when state is " << state_);

  if (conn_ != nullptr) {
    try {
      LockGuard saslGuard(getSaslMutex());
      sasl_dispose( &conn_);
      conn_ = NULL;
    } catch (const LockFailure& e) {
      return Status::MutexError("mutex that guards sasl_dispose unable to lock");
    }
  }

  return Status::OK();
}

//////////////////////////////////////////////////
// Internal callbacks, for sasl_init_client().  //
// Mostly lifted from cyrus' sample_client.c .  //
// Implicitly called in a context that already  //
// holds the SASL/GSSAPI lock.                  //
//////////////////////////////////////////////////

static int
sasl_my_log(void *context __attribute__((unused)),
        int   priority,
        const char *message)
{
  if (! message)
    return SASL_BADPARAM;

  //TODO: get client, connection ID in here
  switch (priority) {
  case SASL_LOG_NONE: return SASL_OK; // no-op
  case SASL_LOG_ERR:  // fall through to FAIL
  case SASL_LOG_FAIL:
    LOG_ERROR(kRPC, << "SASL Error: " << message);
    break;
  case SASL_LOG_WARN:
    LOG_ERROR(kRPC, << message);
    break;
  case SASL_LOG_NOTE:
    LOG_INFO(kRPC, << message);
    break;
  case SASL_LOG_DEBUG:
    LOG_DEBUG(kRPC, << message);
    break;
  case SASL_LOG_TRACE:
    LOG_TRACE(kRPC, << message);
    break;
  case SASL_LOG_PASS: return SASL_OK; // don't log password-info
  default:
    LOG_WARN(kRPC, << "Unknown SASL log level(" << priority << "): " << message);
    break;
  }

  return SASL_OK;
} // sasl_my_log() callback

static int
sasl_getopt(void *context, const char *plugin_name,
               const char *option,
               const char **result, unsigned *len)
{
  if (plugin_name) {
    LOG_WARN(kRPC, << "CySaslEngine: Unexpected plugin_name " << plugin_name);
    return SASL_OK;
  }                   //   123456789012345678
  if (! strncmp( option,  "canon_user_plugin", 18)) {
    // TODO: maybe write a canon_user_plugin to do user-to-principal mapping
    *result = "INTERNAL";
    if (len) *len = strlen( *result);
    return SASL_OK;
  }                   //  12345678901234567
  if (! strncmp( option, "client_mech_list", 17)) {
    *result = "GSSAPI";
    if (len) *len = strlen( *result);
    return SASL_OK;
  }

  (void) context;   // unused
  return SASL_OK; }

#define PLUGINDIR "/usr/local/lib/sasl2" // where the mechanisms are

static int
get_path(void *context, const char ** path)
{
  const char *searchpath = (const char *) context;

  if (! path)
    return SASL_BADPARAM;

  // TODO: check the SASL_PATH environment, or will Cyrus pass that in in the context?
  if (searchpath) {
      *path = searchpath;
  } else {
      *path = PLUGINDIR;
  }

  return SASL_OK;
} // getpath() callback

int get_name(void *context,
             int id,
             const char **result,
             unsigned *len)
{
  const CySaslEngine * pThis = (const CySaslEngine *) context;

  if (!result)
    return SASL_BADPARAM;

  switch (id) {
    case SASL_CB_AUTHNAME:
      if (!pThis->id_)
        break;
      if (len)
        *len = pThis->id_->size();
      *result = pThis->id_->c_str();
      break;
    case SASL_CB_USER:
      if (!pThis->principal_)
        break;
      if (len)
        *len = pThis->principal_->size();
      *result = pThis->principal_->c_str();
      break;
    case SASL_CB_LANGUAGE:
      *result = NULL;
      if (len)
        *len = 0;
      break;
    default:
      return SASL_BADPARAM;
  }

  LOG_DEBUG(kRPC, << "Cyrus::get_name: returning " << *result);

  return SASL_OK;
} // simple() callback

int getrealm(void *context,
             int id,
             const char **availrealms,
             const char **result)
{
  (void)availrealms; // unused
  const CySaslEngine * pThis = (const CySaslEngine *) context;

  if (!result)
    return SASL_BADPARAM;

  if (id != SASL_CB_GETREALM) return SASL_FAIL;
  if (pThis->realm_)
    *result = pThis->realm_->c_str();

  return SASL_OK;
} // getrealm() callback


/*****************************************************************************
*        CYRUS PER-PROCESS INITIALIZATION
*/


const sasl_callback_t per_process_callbacks[] = {
    { SASL_CB_LOG, (sasl_callback_ft) & sasl_my_log, NULL},
    { SASL_CB_GETOPT, (sasl_callback_ft) & sasl_getopt, NULL},
    { SASL_CB_GETPATH, (sasl_callback_ft) & get_path, NULL}, // to find th mechanisms
    { SASL_CB_LIST_END, (sasl_callback_ft) NULL, NULL}
  }; // callbacks_ array

CyrusPerProcessData::CyrusPerProcessData()
{
  try {
    LockGuard saslGuard(getSaslMutex());
    int init_rc = sasl_client_init(per_process_callbacks);
    init_status_ = make_status(init_rc);
  } catch (const LockFailure& e) {
    init_status_ = Status::MutexError("mutex protecting process-wide sasl_client_init unable to lock");
  }
}

CyrusPerProcessData::~CyrusPerProcessData()
{
  // Undo sasl_client_init())
  try {
    LockGuard saslGuard(getSaslMutex());
    sasl_done();
  } catch (const LockFailure& e) {
    // Not can be done at this point, but the process is most likely shutting down anyway.
    LOG_ERROR(kRPC, << "mutex protecting process-wide sasl_done unable to lock");
  }

}

Status CyrusPerProcessData::Init()
{
  return GetInstance().init_status_;
}

CyrusPerProcessData & CyrusPerProcessData::GetInstance()
{
  // Meyer's singleton, thread safe and lazily initialized in C++11
  //
  // Must be lazily initialized to allow client code to plug in a GSSAPI mutex
  // implementation.
  static CyrusPerProcessData per_process_data;
  return per_process_data;
}


} // namespace hdfs
