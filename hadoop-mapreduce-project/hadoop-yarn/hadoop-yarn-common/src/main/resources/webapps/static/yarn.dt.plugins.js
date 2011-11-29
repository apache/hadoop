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
