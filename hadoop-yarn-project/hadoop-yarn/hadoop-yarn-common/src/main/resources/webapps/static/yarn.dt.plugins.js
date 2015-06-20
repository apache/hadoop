
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


if (!jQuery.fn.dataTableExt.fnVersionCheck("1.7.5")) {
  alert("These plugins requires dataTables 1.7.5+");
}

// don't filter on hidden html elements for an sType of title-numeric
$.fn.dataTableExt.ofnSearch['title-numeric'] = function ( sData ) {
   return sData.replace(/\n/g," ").replace( /<.*?>/g, "" );
}

// 'title-numeric' sort type
jQuery.fn.dataTableExt.oSort['title-numeric-asc']  = function(a,b) {
  var x = a.match(/title=["']?(-?\d+\.?\d*)/)[1];
  var y = b.match(/title=["']?(-?\d+\.?\d*)/)[1];
  x = parseFloat( x );
  y = parseFloat( y );
  return ((x < y) ? -1 : ((x > y) ?  1 : 0));
};

jQuery.fn.dataTableExt.oSort['title-numeric-desc'] = function(a,b) {
  var x = a.match(/title=["']?(-?\d+\.?\d*)/)[1];
  var y = b.match(/title=["']?(-?\d+\.?\d*)/)[1];
  x = parseFloat( x );
  y = parseFloat( y );
  return ((x < y) ?  1 : ((x > y) ? -1 : 0));
};

jQuery.fn.dataTableExt.oApi.fnSetFilteringDelay = function ( oSettings, iDelay ) {
  var
  _that = this,
  iDelay = (typeof iDelay == 'undefined') ? 250 : iDelay;

  this.each( function ( i ) {
    $.fn.dataTableExt.iApiIndex = i;
    var
    $this = this,
    oTimerId = null,
    sPreviousSearch = null,
    anControl = $( 'input', _that.fnSettings().aanFeatures.f );

    anControl.unbind( 'keyup' ).bind( 'keyup', function() {
      var $$this = $this;

      if (sPreviousSearch === null || sPreviousSearch != anControl.val()) {
        window.clearTimeout(oTimerId);
        sPreviousSearch = anControl.val();
        oSettings.oApi._fnProcessingDisplay(oSettings, true);
        oTimerId = window.setTimeout(function() {
          $.fn.dataTableExt.iApiIndex = i;
          _that.fnFilter( anControl.val() );
          oSettings.oApi._fnProcessingDisplay(oSettings, false);
        }, iDelay);
      }
    });
    return this;
  } );
  return this;
}

function renderHadoopDate(data, type, full) {
  if (type === 'display' || type === 'filter') {
    if(data === '0'|| data === '-1') {
      return "N/A";
    }
    var date = new Date(parseInt(data));
    var monthList = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                     "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    var weekdayList = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
    var offsetMinutes = date.getTimezoneOffset();
    var offset
    if (offsetMinutes <= 0) {
      offset = "+" + zeroPad(-offsetMinutes / 60 * 100, 4);
    } else {
      offset = "-" + zeroPad(offsetMinutes / 60 * 100, 4);
    }

    // EEE MMM dd HH:mm:ss Z yyyy
    return weekdayList[date.getDay()] + " " +
        monthList[date.getMonth()] + " " +
        date.getDate() + " " +
        zeroPad(date.getHours(), 2) + ":" +
        zeroPad(date.getMinutes(), 2) + ":" +
        zeroPad(date.getSeconds(), 2) + " " +
        offset + " " +
        date.getFullYear();
  }
  // 'sort', 'type' and undefined all just use the number
  // If date is 0, then for purposes of sorting it should be consider max_int
  return data === '0' ? '9007199254740992' : data;  
}

function zeroPad(n, width) {
  n = n + '';
  return n.length >= width ? n : new Array(width - n.length + 1).join('0') + n;
}

function renderHadoopElapsedTime(data, type, full) {
  if (type === 'display' || type === 'filter') {
    var timeDiff = parseInt(data);
    if(timeDiff < 0)
      return "N/A";
    
    var hours = Math.floor(timeDiff / (60*60*1000));
    var rem = (timeDiff % (60*60*1000));
    var minutes =  Math.floor(rem / (60*1000));
    rem = rem % (60*1000);
    var seconds = Math.floor(rem / 1000);
    
    var toReturn = "";
    if (hours != 0){
      toReturn += hours;
      toReturn += "hrs, ";
    }
    if (minutes != 0){
      toReturn += minutes;
      toReturn += "mins, ";
    }
    toReturn += seconds;
    toReturn += "sec";
    return toReturn;
  }
  // 'sort', 'type' and undefined all just use the number
  return data;  
}

//JSON array element is formatted like
//"<a href="/proxy/application_1360183373897_0001>">application_1360183373897_0001</a>"
//this function removes <a> tag and return a string value for sorting
function parseHadoopID(data, type, full) {
  if (type === 'display') {
    return data;
  }

  var splits =  data.split('>');
  // Return original string if there is no HTML tag
  if (splits.length === 1) return data;

  //Return the visible string rather than the entire HTML tag
  return splits[1].split('<')[0];
}

function parseHadoopProgress(data, type, full) {
  if (type === 'display') {
    return data;
  }
  //Return the title attribute for 'sort', 'filter', 'type' and undefined
  return data.split("'")[1];
}
