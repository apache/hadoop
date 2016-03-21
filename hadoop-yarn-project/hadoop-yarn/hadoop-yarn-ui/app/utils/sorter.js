import Converter from 'yarn-ui/utils/converter';
import Ember from 'ember';

export default {
  _initElapsedTimeSorter: function() {
    Ember.$.extend(Ember.$.fn.dataTableExt.oSort, {
      "elapsed-time-pre": function (a) {
         return Converter.padding(Converter.elapsedTimeToMs(a), 20);
      },
    });
  },

  _initNaturalSorter: function() {
    Ember.$.extend(Ember.$.fn.dataTableExt.oSort, {
      "natural-asc": function (a, b) {
        return naturalSort(a,b);
      },
      "natural-desc": function (a, b) {
        return naturalSort(a,b) * -1;
      },
    });
  },

  initDataTableSorter: function() {
    this._initElapsedTimeSorter();
    this._initNaturalSorter();
  },
};

/**
 * Natural sort implementation.
 * Typically used to sort application Ids'.
 */
function naturalSort(a, b) {
  var diff = a.length - b.length;
  if (diff != 0) {
    var splitA = a.split("_");
    var splitB = b.split("_");
    if (splitA.length != splitB.length) {
      return a.localeCompare(b);
    }
    for (var i = 1; i < splitA.length; i++) {
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
