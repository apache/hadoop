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

(function() {
  function naturalSort (a, b) {
    var diff = a.length - b.length;
    if (diff != 0) {
      var splitA = a.split("_");
      var splitB = b.split("_");
      if (splitA.length != splitB.length) {
        return a.localeCompare(b);
      }
      for (var i=1; i < splitA.length; i++) {
        var splitdiff = splitA[i].length - splitB[i].length;
        if (splitdiff != 0) {
          return splitdiff;
        }
        var splitCompare = splitA[i].localeCompare(splitB[i]);
        if (splitCompare != 0) {
          return splitCompare;
        }
      }
      return diff;
    }
    return a.localeCompare(b);
  }

jQuery.extend( jQuery.fn.dataTableExt.oSort, {
  "natural-asc": function ( a, b ) {
    return naturalSort(a,b);
  },

  "natural-desc": function ( a, b ) {
    return naturalSort(a,b) * -1;
  }
} );

}());

