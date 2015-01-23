/* Module for handling the spatial querying
 */

this.ckan.spatial_libs = this.ckan.spatial_libs || {}

// template lib for documentation purposes
this.ckan.spatial_libs.template = {
    geojson2extent: function(geojson) { },
    geom2bboxstring: function(geom) {},
    extent2bboxstring: function(extent) {},
    drawExtentFromCoords: function(xmin, ymin, xmax, ymax) {},
    drawExtentFromGeoJSON: function(geom) {},
    createMap: function(container, config, enableDraw) {
        return {
            onDrawEnable: function(callback) {},
            onSelect: function(callback) {},
            onMoveEnd: function(callback) {},
            setSelectedGeom: function(geom, updateExtent) {},
            zoomIn: function() {},
            reset: function() {},
            clearSelection: function() {},
            fitToExtent: function(minx, miny, maxx, maxy) {},
            fitToSelection: function() {},
            getSelection: function() {},
            getExtent: function() {}
        }
    }
}


this.ckan.module('spatial-query', function ($, _) {

  return {
    options: {
      i18n: {
      },
      default_extent: [[90, 180], [-90, -180]]
    },

    template: {
      buttons: [
        '<div id="dataset-map-edit-buttons">',
        '<a href="javascript:;" class="btn cancel">Cancel</a> ',
        '<a href="javascript:;" class="btn apply disabled">Apply</a>',
        '</div>'
      ].join('')
    },

    initialize: function () {
      var module = this;
      $.proxyAll(this, /_on/);

      var libname = this.options.map_config.spatial_lib || 'leaflet'
      this.spatial_lib = ckan.spatial_libs[libname]
      if (! this.spatial_lib) throw "Spatial lib implementation not found: "+libname

      var user_default_extent = this.el.data('default_extent');
      if (user_default_extent ){
        if (user_default_extent instanceof Array) {
          // Assume it's a pair of coords like [[90, 180], [-90, -180]]
          this.options.default_extent = user_default_extent;
        } else if (user_default_extent instanceof Object) {
          // Assume it's a GeoJSON bbox
          this.options.default_extent = this.spatial_lib.geojson2extent(user_default_extent)
        }
      }
      this.el.ready(this._onReady);
    },

    _getParameterByName: function (name) {
      var match = RegExp('[?&]' + name + '=([^&]*)')
                        .exec(window.location.search);
      return match ?
          decodeURIComponent(match[1].replace(/\+/g, ' '))
          : null;
    },

    _drawExtentFromCoords: function(xmin, ymin, xmax, ymax) {
        if ($.isArray(xmin)) {
            var coords = xmin;
            xmin = coords[0]; ymin = coords[1]; xmax = coords[2]; ymax = coords[3];
        }
        return this.spatial_lib.drawExtentFromCoords(xmin, ymin, xmax, ymax);
    },

    _drawExtentFromGeoJSON: function(geom) {
        return this.spatial_lib.drawExtentFromGeoJSON(geom);
    },

    _onReady: function() {
      var module = this;
      var map;
      var previous_extent;
      var is_exanded = false;
      var form = $("#dataset-search");
      // CKAN 2.1
      if (!form.length) {
          form = $(".search-form");
      }
      // looking for mini side search
      if (!form.length) {
          form = $(".form-search");
      }

      var buttons;

      // Add necessary fields to the search form if not already created
      $(['ext_bbox', 'ext_prev_extent']).each(function(index, item){
        if ($("#" + item).length === 0) {
          $('<input type="hidden" />').attr({'id': item, 'name': item}).appendTo(form);
        }
      });

      // OK map time
      var spatial_lib = this.spatial_lib
      map = spatial_lib.createMap('dataset-map-container', this.options.map_config, true)

      // OK add the expander
      map.onDrawEnable(function(e) {
        if (!is_exanded) {
          $('body').addClass('dataset-map-expanded');
          map.reset();
          is_exanded = true;
        }
      })

      // Setup the expanded buttons
      buttons = $(module.template.buttons).insertAfter(module.el);

      // Handle the cancel expanded action
      $('.cancel', buttons).on('click', function() {
        $('body').removeClass('dataset-map-expanded');
        map.clearSelection()
        setPreviousExtent();
        setPreviousBBBox();
        map.reset();
        is_exanded = false;
      });

      // Handle the apply expanded action
      $('.apply', buttons).on('click', function() {
        if (map.getSelection()) {
          $('body').removeClass('dataset-map-expanded');
          is_exanded = false;
          map.reset()
          // Eugh, hacky hack.
          setTimeout(function() {
            map.fitToSelection();
            submitForm();
          }, 200);
        }
      });

      // When user finishes drawing the box, record it and add it to the map
      map.onSelect(function(geom) {
        $('#ext_bbox').val(spatial_lib.geom2bboxstring(geom));
        $('.apply', buttons).removeClass('disabled').addClass('btn-primary');
      })

      // Record the current map view so we can replicate it after submitting
      map.onMoveEnd( function(e) {
        $('#ext_prev_extent').val(spatial_lib.extent2bboxstring(map.getExtent()));
      });

      // Ok setup the default state for the map
      var previous_bbox;
      setPreviousBBBox();
      setPreviousExtent();


      // Is there an existing box from a previous search?
      function setPreviousBBBox() {
        previous_bbox = module._getParameterByName('ext_bbox');
        if (previous_bbox) {
          $('#ext_bbox').val(previous_bbox);
          var previousBBox = module._drawExtentFromCoords(previous_bbox.split(','))
          map.setSelectedGeom(previousBBox)
          map.fitToSelection()
        }
      }

      // Is there an existing extent from a previous search?
      function setPreviousExtent() {
        previous_extent = module._getParameterByName('ext_prev_extent');
        if (previous_extent) {
          coords = previous_extent.split(',');
          map.fitToExtent(parseFloat(coords[0]), parseFloat(coords[1]), parseFloat(coords[2]), parseFloat(coords[3]));
        } else {
          if (!previous_bbox){
              map.fitToExtent(
                  module.options.default_extent[0][1],
                  module.options.default_extent[0][0],
                  module.options.default_extent[1][1],
                  module.options.default_extent[1][0]);
          }
        }
      }

      // Add the loading class and submit the form
      function submitForm() {
        setTimeout(function() {
          form.submit();
        }, 800);
      }
    }
  }
});
