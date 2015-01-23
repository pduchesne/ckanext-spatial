/* Module for handling the spatial querying
 */

this.ckan.spatial_libs = this.ckan.spatial_libs || {}

this.ckan.spatial_libs.leaflet = function() {
    return {
        style: {
            color: '#F06F64',
            weight: 2,
            opacity: 1,
            fillColor: '#F06F64',
            fillOpacity: 0.1
        },

        geojson2extent: function(geojson) {
            return new L.GeoJSON(geojson).getBounds()
        },

        geom2bboxstring: function(geom) {
            return this.extent2bboxstring(geom.getBounds())
        },

        extent2bboxstring: function(extent) {
            return extent.toBBoxString()
        },

        drawExtentFromCoords: function(xmin, ymin, xmax, ymax) {
            return new L.Rectangle([[ymin, xmin], [ymax, xmax]], this.style);
        },

        drawExtentFromGeoJSON: function(geom) {
            return new L.GeoJSON(geom, {style: this.style});
        },

        createMap: function(container, config, enableDraw) {
            var map = ckan.commonLeafletMap(container, config, {attributionControl: false});

            var mapComponent = {
                _selectionListener: null,
                _should_zoom: true,
                _selectedGeom : null,
                _map: map,

                init: function() {
                    // OK, when we expand we shouldn't zoom then
                    var _this = this
                    this._map.on('zoomstart', function(e) {
                        _this._should_zoom = false;
                    });
                },

                onSelect: function(listener) {
                    this._selectionListener = listener
                },

                onDrawEnable: function(callback) {
                    var _this = this
                    $('.leaflet-control-draw a', this._map.parentElement).on('click', function() {
                        if (_this._should_zoom && !_this._map.getSelection()) {
                            map.zoomIn();
                        }
                        callback()
                    });
                },
                onMoveEnd: function(callback) {
                    this._map.on('moveend',callback)
                },
                setSelectedGeom: function(geom, updateExtent) {
                    if (this._selectedGeom) {
                        this._map.removeLayer(this._selectedGeom)
                    }
                    this._selectedGeom = geom
                    this._map.addLayer(this._selectedGeom)

                    if (updateExtent) this.fitToSelection
                },
                zoomIn: function() {
                    this._map.zoomIn()
                },
                reset: function() {
                    L.Util.requestAnimFrame(this._map.invalidateSize, this._map, !1, this._map._container);
                },
                clearSelection: function() {
                    if (this._selectedGeom) {
                        this._map.removeLayer(this._selectedGeom);
                    }
                },
                fitToExtent: function(minx, miny, maxx, maxy) {
                    this._map.fitBounds([[miny, minx],[maxy, maxx]]);
                },
                fitToSelection: function() {
                    this._map.fitBounds(this._selectedGeom.getBounds());
                },
                getSelection: function() {
                    return this._selectedGeom
                },
                getExtent: function() {
                    return this._map.getBounds()
                }
            }

            mapComponent.init()

            // Initialize the draw control
            if (enableDraw) {
                map.addControl(new L.Control.Draw({
                    position: 'topright',
                    polyline: false, polygon: false,
                    circle: false, marker: false,
                    rectangle: {
                        shapeOptions: this.style,
                        title: 'Draw rectangle'
                    }
                }))

                map.on('draw:rectangle-created', function (e) {
                    mapComponent.setSelectedGeom(e.rect)
                    mapComponent._selectionListener && mapComponent._selectionListener(e.rect)
                });
            }

            return mapComponent
        }
    }
} ()
