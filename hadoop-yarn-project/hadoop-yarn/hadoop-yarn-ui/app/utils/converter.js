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
  dateToTimeStamp: function(date) {
    if (date) {
      var ts = moment(date, "YYYY/MM/DD HH:mm:ss").valueOf();
      return ts;
    }
  }
}
