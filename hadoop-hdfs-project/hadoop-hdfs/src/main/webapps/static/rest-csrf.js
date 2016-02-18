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

"use strict";

// Initializes client-side handling of cross-site request forgery (CSRF)
// protection by figuring out the custom HTTP headers that need to be sent in
// requests and which HTTP methods are ignored because they do not require CSRF
// protection.
(function() {
  var restCsrfCustomHeader = null;
  var restCsrfMethodsToIgnore = null;

  $.ajax({'url': '/conf', 'dataType': 'xml', 'async': false}).done(
    function(data) {
      function getBooleanValue(element) {
        return ($(element).find('value').text().trim().toLowerCase() === 'true')
      }

      function getTrimmedStringValue(element) {
        return $(element).find('value').text().trim();
      }

      function getTrimmedStringArrayValue(element) {
        var str = $(element).find('value').text().trim();
        var array = [];
        if (str) {
          var splitStr = str.split(',');
          for (var i = 0; i < splitStr.length; i++) {
            array.push(splitStr[i].trim());
          }
        }
        return array;
      }

      // Get all relevant configuration properties.
      var $xml = $(data);
      var csrfEnabled = false;
      var header = null;
      var methods = [];
      $xml.find('property').each(function(idx, element) {
        var name = $(element).find('name').text();
        if (name === 'dfs.webhdfs.rest-csrf.enabled') {
          csrfEnabled = getBooleanValue(element);
        } else if (name === 'dfs.webhdfs.rest-csrf.custom-header') {
          header = getTrimmedStringValue(element);
        } else if (name === 'dfs.webhdfs.rest-csrf.methods-to-ignore') {
          methods = getTrimmedStringArrayValue(element);
        }
      });

      // If enabled, set up all subsequent AJAX calls with a pre-send callback
      // that adds the custom headers if necessary.
      if (csrfEnabled) {
        restCsrfCustomHeader = header;
        restCsrfMethodsToIgnore = {};
        methods.map(function(method) { restCsrfMethodsToIgnore[method] = true; });
        $.ajaxSetup({
          beforeSend: addRestCsrfCustomHeader
        });
      }
    });

  // Adds custom headers to request if necessary.  This is done only for WebHDFS
  // URLs, and only if it's not an ignored method.
  function addRestCsrfCustomHeader(xhr, settings) {
    if (settings.url == null || !settings.url.startsWith('/webhdfs/')) {
      return;
    }
    var method = settings.type;
    if (restCsrfCustomHeader != null && !restCsrfMethodsToIgnore[method]) {
      // The value of the header is unimportant.  Only its presence matters.
      xhr.setRequestHeader(restCsrfCustomHeader, '""');
    }
  }
})();
