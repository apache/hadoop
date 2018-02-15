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

export default {
  getErrorTypeByErrorCode: function(code) {
    var errorType = '';
    if (code) {
      switch (code) {
        case "500":
          errorType = "Internal Server Error";
          break;
        case "502":
          errorType = "Bad Gateway";
          break;
        case "503":
          errorType = "Service Unavailable";
          break;
        case "400":
          errorType = "Bad Request";
          break;
        case "403":
          errorType = "Forbidden";
          break;
        case "404":
          errorType = "Not Found";
          break;
        case "401":
          errorType = "Authorization required";
          break;
        default:
          errorType = "";
          break;
      }
    }
    return errorType;
  },
  stripErrorCodeAndMessageFromError: function(err) {
    var obj = {};
    if (err && err.errors && err.errors[0]) {
      obj.errorCode = err.errors[0].status || "";
      obj.title = err.errors[0].title || "";
      obj.errorType = this.getErrorTypeByErrorCode(err.errors[0].status);
    }
    return obj;
  }
};
