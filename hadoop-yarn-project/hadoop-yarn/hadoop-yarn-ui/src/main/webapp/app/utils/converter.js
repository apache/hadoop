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

import Constants from 'yarn-ui/constants';

export default {
  containerIdToAttemptId: function(containerId) {
    if (containerId) {
      var arr = containerId.split('_');
      var attemptId = ["appattempt", arr[1],
        arr[2], this.padding(arr[3], 6)];
      return attemptId.join('_');
    }
  },
  attemptIdToAppId: function(attemptId) {
    if (attemptId) {
      var arr = attemptId.split('_');
      var appId = ["application", arr[1],
        arr[2]].join('_');
      return appId;
    }
  },
  padding: function(str, toLen=2) {
    str = str.toString();
    if (str.length >= toLen) {
      return str;
    }
    return '0'.repeat(toLen - str.length) + str;
  },
  resourceToString: function(mem, cpu) {
    mem = Math.max(0, mem);
    cpu = Math.max(0, cpu);
    return mem + " MBs, " + cpu + " VCores";
  },
  msToElapsedTime: function(timeInMs) {
    var sec_num = timeInMs / 1000; // don't forget the second param
    var hours = Math.floor(sec_num / 3600);
    var minutes = Math.floor((sec_num - (hours * 3600)) / 60);
    var seconds = sec_num - (hours * 3600) - (minutes * 60);

    var timeStrArr = [];

    if (hours > 0) {
      timeStrArr.push(hours + ' Hrs');
    }
    if (minutes > 0) {
      timeStrArr.push(minutes + ' Mins');
    }
    if (seconds > 0) {
      timeStrArr.push(Math.round(seconds) + " Secs");
    }
    return timeStrArr.join(' : ');
  },
  elapsedTimeToMs: function(elapsedTime) {
    elapsedTime = elapsedTime.toLowerCase();
    var arr = elapsedTime.split(' : ');
    var total = 0;
    for (var i = 0; i < arr.length; i++) {
      if (arr[i].indexOf('hr') > 0) {
        total += parseInt(arr[i].substring(0, arr[i].indexOf(' '))) * 3600;
      } else if (arr[i].indexOf('min') > 0) {
        total += parseInt(arr[i].substring(0, arr[i].indexOf(' '))) * 60;
      } else if (arr[i].indexOf('sec') > 0) {
        total += parseInt(arr[i].substring(0, arr[i].indexOf(' ')));
      }
    }
    return total * 1000;
  },
  timeStampToDate: function(timeStamp) {
    var dateTimeString = moment(parseInt(timeStamp)).format("YYYY/MM/DD HH:mm:ss");
    return dateTimeString;
  },
  timeStampToDateOnly: function(timeStamp) {
    var dateTimeString = moment(parseInt(timeStamp)).format("YYYY/MM/DD");
    return dateTimeString;
  },
  dateToTimeStamp: function(date) {
    if (date) {
      var ts = moment(date, "YYYY/MM/DD HH:mm:ss").valueOf();
      return ts;
    }
  },
  isoDateToDate: function(isoDate) {
    return moment(isoDate).format("YYYY/MM/DD HH:mm:ss");
  },
  splitForContainerLogs: function(id) {
    if (id) {
      var splits = id.split(Constants.PARAM_SEPARATOR);
      var splitLen = splits.length;
      if (splitLen < 3) {
        return null;
      }
      var fileName = splits[2];
      var index;
      for (index = 3; index < splitLen; index++) {
        fileName = fileName + Constants.PARAM_SEPARATOR + splits[index];
      }
      return [splits[0], splits[1], fileName];
    }
  },
  memoryToSimpliedUnit: function(mb) {
    var unit = "MB";
    var value = mb;
    if (value / 1024 >= 0.9) {
      value = value / 1024;
      unit = "GB";
    }
    if (value / 1024 >= 0.9) {
      value = value / 1024;
      unit = "TB";
    }
    if (value / 1024 >= 0.9) {
      value = value / 1024;
      unit = "PB";
    }
    return value.toFixed(1) + " " + unit;
  },
  resourceToSimplifiedUnit: function (value, unit) {
    // First convert unit to base unit ("").
    var normalizedValue = value;
    if (unit === "Ki") {
      normalizedValue = normalizedValue * 1024;
    } else if (unit === "Mi") {
      normalizedValue = normalizedValue * 1024 * 1024;
    } else if (unit === "Gi") {
      normalizedValue = normalizedValue * 1024 * 1024 * 1024;
    } else if (unit === "Ti") {
      normalizedValue = normalizedValue * 1024 * 1024 * 1024 * 1024;
    } else if (unit === "Pi") {
      normalizedValue = normalizedValue * 1024 * 1024 * 1024 * 1024 * 1024;
    } else if (unit === "K" || unit === "k") {
      normalizedValue = normalizedValue * 1000;
    } else if (unit === "M" || unit === "m") {
      normalizedValue = normalizedValue * 1000 * 1000;
    } else if (unit === "G" || unit === "g") {
      normalizedValue = normalizedValue * 1000 * 1000 * 1000;
    } else if (unit === "T" || unit === "t") {
      normalizedValue = normalizedValue * 1000 * 1000 * 1000 * 1000;
    } else if (unit === "P" || unit === "p") {
      normalizedValue = normalizedValue * 1000 * 1000 * 1000 * 1000 * 1000;
    }

    // From baseunit ("") convert to most human readable unit
    // (which value < 1024 * 0.9).
    var finalUnit = "";
    if (normalizedValue / 1024 >= 0.9) {
      normalizedValue = normalizedValue / 1024;
      finalUnit = "Ki";
    }
    if (normalizedValue / 1024 >= 0.9) {
      normalizedValue = normalizedValue / 1024;
      finalUnit = "Mi";
    }
    if (normalizedValue / 1024 >= 0.9) {
      normalizedValue = normalizedValue / 1024;
      finalUnit = "Gi";
    }
    if (normalizedValue / 1024 >= 0.9) {
      normalizedValue = normalizedValue / 1024;
      finalUnit = "Ti";
    }
    if (normalizedValue / 1024 >= 0.9) {
      normalizedValue = normalizedValue / 1024;
      finalUnit = "Pi";
    }

    return normalizedValue.toFixed(1) + " " + finalUnit;
  },
  msToElapsedTimeUnit: function(millisecs, short) {
    var seconds = Math.floor(millisecs / 1000);
    var days = Math.floor(seconds / (3600 * 24));
    var hours = Math.floor(seconds / 3600) - (days * 24);
    var mins = Math.floor((seconds - (hours * 3600) - (days * 24 * 3600)) / 60);
    var secs = seconds - (days * 24 * 3600) - (hours * 3600) - (mins * 60);
    var timeStrArr = [];
    var pluralize = "";
    if (days > 0) {
      pluralize = days > 1? " Days" : " Day";
      timeStrArr.push(days + pluralize);
    }
    if (hours > 0) {
      pluralize = hours > 1? " Hrs" : " Hour";
      timeStrArr.push(hours + pluralize);
    }
    if (mins > 0) {
      pluralize = mins > 1? " Mins" : " Min";
      timeStrArr.push(mins + pluralize);
    }
    pluralize = secs > 1? " Secs" : " Sec";
    timeStrArr.push(secs + pluralize);
    if (short) {
      return timeStrArr[0] + (timeStrArr[1]? " : " + timeStrArr[1] : "");
    }
    return timeStrArr.join(" : ");
  },
  memoryBytesToMB: function(mem) {
    var unit = "MB";
    var value = mem / (1024 * 1024);
    if (value / 1024 >= 0.9) {
      value = value / 1024;
      unit = "GB";
    }
    return value.toFixed(1) + " " + unit;
  },
  floatToFixed: function(value, fixed=2) {
    if (value && value.toFixed) {
      return parseFloat(value.toFixed(fixed));
    }
    return value;
  }
};
