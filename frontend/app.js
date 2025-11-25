(function() {
    'use strict';

    // Configuration
    var API_BASE = window.API_BASE || 'http://localhost:8000';
    var MONTREAL_CENTER = [45.5017, -73.5673];
    var DEFAULT_ZOOM = 11;

    // State
    var map = null;
    var areaLayers = {};
    var scores = [];
    var selectedArea = null;

    // Montreal borough approximate boundaries (simplified polygons)
    var BOROUGH_BOUNDS = {
        'Ahuntsic-Cartierville': [[45.53, -73.72], [45.53, -73.64], [45.58, -73.64], [45.58, -73.72]],
        'Anjou': [[45.59, -73.58], [45.59, -73.54], [45.62, -73.54], [45.62, -73.58]],
        'Cote-des-Neiges-Notre-Dame-de-Grace': [[45.47, -73.65], [45.47, -73.58], [45.51, -73.58], [45.51, -73.65]],
        'Lachine': [[45.42, -73.72], [45.42, -73.66], [45.46, -73.66], [45.46, -73.72]],
        'LaSalle': [[45.40, -73.68], [45.40, -73.60], [45.45, -73.60], [45.45, -73.68]],
        'Le Plateau-Mont-Royal': [[45.51, -73.60], [45.51, -73.56], [45.54, -73.56], [45.54, -73.60]],
        'Le Sud-Ouest': [[45.45, -73.60], [45.45, -73.55], [45.49, -73.55], [45.49, -73.60]],
        'Mercier-Hochelaga-Maisonneuve': [[45.54, -73.56], [45.54, -73.50], [45.59, -73.50], [45.59, -73.56]],
        'Montreal-Nord': [[45.58, -73.66], [45.58, -73.62], [45.62, -73.62], [45.62, -73.66]],
        'Outremont': [[45.51, -73.62], [45.51, -73.59], [45.53, -73.59], [45.53, -73.62]],
        'Pierrefonds-Roxboro': [[45.47, -73.88], [45.47, -73.80], [45.52, -73.80], [45.52, -73.88]],
        'Riviere-des-Prairies-Pointe-aux-Trembles': [[45.62, -73.58], [45.62, -73.48], [45.68, -73.48], [45.68, -73.58]],
        'Rosemont-La Petite-Patrie': [[45.54, -73.62], [45.54, -73.56], [45.57, -73.56], [45.57, -73.62]],
        'Saint-Laurent': [[45.48, -73.72], [45.48, -73.66], [45.53, -73.66], [45.53, -73.72]],
        'Saint-Leonard': [[45.58, -73.62], [45.58, -73.56], [45.61, -73.56], [45.61, -73.62]],
        'Verdun': [[45.44, -73.58], [45.44, -73.54], [45.47, -73.54], [45.47, -73.58]],
        'Ville-Marie': [[45.49, -73.58], [45.49, -73.54], [45.52, -73.54], [45.52, -73.58]],
        'Villeray-Saint-Michel-Parc-Extension': [[45.54, -73.66], [45.54, -73.60], [45.58, -73.60], [45.58, -73.66]]
    };

    function init() {
        initMap();
        loadScores();
        bindEvents();
    }

    function initMap() {
        map = L.map('map').setView(MONTREAL_CENTER, DEFAULT_ZOOM);

        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: '&copy; OpenStreetMap contributors'
        }).addTo(map);
    }

    function loadScores() {
        fetch(API_BASE + '/scores/latest')
            .then(function(res) { return res.json(); })
            .then(function(data) {
                scores = data;
                renderAreas();
                renderAreaList();
            })
            .catch(function(err) {
                console.error('Failed to load scores:', err);
                renderMockData();
            });
    }

    function renderMockData() {
        // Fallback mock data for demo
        scores = Object.keys(BOROUGH_BOUNDS).map(function(name, i) {
            var score = Math.floor(Math.random() * 60) + 30;
            return {
                area_id: name.toLowerCase().replace(/[^a-z0-9]/g, '-'),
                borough_name: name,
                safety_score: score,
                color: score >= 70 ? 'green' : (score >= 40 ? 'yellow' : 'red'),
                crime_count: Math.floor(Math.random() * 100),
                accident_count: Math.floor(Math.random() * 50),
                period: '2024-01'
            };
        });
        renderAreas();
        renderAreaList();
    }

    function renderAreas() {
        // Clear existing layers
        Object.keys(areaLayers).forEach(function(key) {
            map.removeLayer(areaLayers[key]);
        });
        areaLayers = {};

        scores.forEach(function(score) {
            var name = score.borough_name || score.area_id;
            var bounds = BOROUGH_BOUNDS[name];

            if (!bounds) {
                // Try to find approximate match
                var matchKey = Object.keys(BOROUGH_BOUNDS).find(function(k) {
                    return k.toLowerCase().indexOf(name.toLowerCase()) >= 0 ||
                           name.toLowerCase().indexOf(k.toLowerCase()) >= 0;
                });
                if (matchKey) {
                    bounds = BOROUGH_BOUNDS[matchKey];
                }
            }

            if (bounds) {
                var color = getColor(score.color);
                var polygon = L.polygon(bounds, {
                    color: color,
                    fillColor: color,
                    fillOpacity: 0.4,
                    weight: 2
                });

                polygon.bindPopup(createPopupContent(score));
                polygon.on('click', function() {
                    selectArea(score.area_id);
                });

                polygon.addTo(map);
                areaLayers[score.area_id] = polygon;
            }
        });
    }

    function getColor(colorName) {
        var colors = {
            'green': '#27ae60',
            'yellow': '#f39c12',
            'red': '#e74c3c'
        };
        return colors[colorName] || colors['yellow'];
    }

    function createPopupContent(score) {
        return '<div class="popup-content">' +
            '<strong>' + (score.borough_name || score.area_id) + '</strong><br>' +
            'Safety Score: ' + Math.round(score.safety_score) + '/100<br>' +
            'Crimes: ' + score.crime_count + '<br>' +
            'Accidents: ' + score.accident_count +
            '</div>';
    }

    function renderAreaList() {
        var container = document.getElementById('area-list');
        var sortedScores = scores.slice().sort(function(a, b) {
            return b.safety_score - a.safety_score;
        });

        var html = sortedScores.map(function(score) {
            return '<div class="area-item" data-area="' + score.area_id + '">' +
                '<span class="name">' + (score.borough_name || score.area_id) + '</span>' +
                '<span class="score ' + score.color + '">' + Math.round(score.safety_score) + '</span>' +
                '</div>';
        }).join('');

        container.innerHTML = html;
    }

    function selectArea(areaId) {
        selectedArea = areaId;

        // Highlight on map
        Object.keys(areaLayers).forEach(function(key) {
            var layer = areaLayers[key];
            if (key === areaId) {
                layer.setStyle({ weight: 4, fillOpacity: 0.6 });
            } else {
                layer.setStyle({ weight: 2, fillOpacity: 0.4 });
            }
        });

        // Load area details
        loadAreaDetail(areaId);
    }

    function loadAreaDetail(areaId) {
        var container = document.getElementById('area-detail');
        container.innerHTML = '<p class="placeholder">Loading...</p>';

        // Get score from local data first
        var localScore = scores.find(function(s) { return s.area_id === areaId; });

        fetch(API_BASE + '/advice/' + areaId)
            .then(function(res) { return res.json(); })
            .then(function(data) {
                renderAreaDetail(data, localScore);
            })
            .catch(function(err) {
                // Fallback to local data
                if (localScore) {
                    renderAreaDetail({
                        area_id: areaId,
                        area_name: localScore.borough_name || areaId,
                        safety_score: localScore.safety_score,
                        advice: getLocalAdvice(localScore)
                    }, localScore);
                } else {
                    container.innerHTML = '<p class="placeholder">Could not load details</p>';
                }
            });
    }

    function getLocalAdvice(score) {
        if (score.safety_score >= 70) {
            return 'This area has good safety metrics. Standard precautions recommended.';
        } else if (score.safety_score >= 40) {
            return 'Moderate safety levels. Stay aware of your surroundings, especially at night.';
        } else {
            return 'Exercise caution in this area. Consider traveling during daylight hours.';
        }
    }

    function renderAreaDetail(data, localScore) {
        var container = document.getElementById('area-detail');
        var score = localScore || {};

        var html = '<div class="detail-content">' +
            '<h3>' + (data.area_name || data.area_id) + '</h3>' +
            '<div class="stats">' +
            '<div class="stat">' +
            '<div class="stat-label">Safety Score</div>' +
            '<div class="stat-value">' + Math.round(data.safety_score || score.safety_score || 0) + '/100</div>' +
            '</div>' +
            '<div class="stat">' +
            '<div class="stat-label">Crime Incidents</div>' +
            '<div class="stat-value">' + (score.crime_count || 'N/A') + '</div>' +
            '</div>' +
            '<div class="stat">' +
            '<div class="stat-label">Accidents</div>' +
            '<div class="stat-value">' + (score.accident_count || 'N/A') + '</div>' +
            '</div>' +
            '<div class="stat">' +
            '<div class="stat-label">Period</div>' +
            '<div class="stat-value">' + (score.period || 'N/A') + '</div>' +
            '</div>' +
            '</div>' +
            '<div class="advice">' + (data.advice || '') + '</div>' +
            '</div>';

        container.innerHTML = html;
    }

    function askQuestion() {
        var input = document.getElementById('question-input');
        var question = input.value.trim();
        var answerBox = document.getElementById('answer-box');

        if (!question) return;

        answerBox.innerHTML = 'Thinking...';

        fetch(API_BASE + '/ask?question=' + encodeURIComponent(question))
            .then(function(res) { return res.json(); })
            .then(function(data) {
                answerBox.innerHTML = data.answer || 'No answer available.';
            })
            .catch(function(err) {
                answerBox.innerHTML = getLocalAnswer(question);
            });
    }

    function getLocalAnswer(question) {
        var q = question.toLowerCase();
        if (q.indexOf('safe') >= 0) {
            return 'The safest areas typically have scores above 70. Check the area list for current rankings.';
        }
        if (q.indexOf('dangerous') >= 0 || q.indexOf('avoid') >= 0) {
            return 'Areas with scores below 40 require extra caution, especially at night.';
        }
        return 'For specific safety information, click on an area in the map or area list.';
    }

    function bindEvents() {
        document.getElementById('area-list').addEventListener('click', function(e) {
            var item = e.target.closest('.area-item');
            if (item) {
                var areaId = item.getAttribute('data-area');
                selectArea(areaId);

                // Pan to area on map
                var layer = areaLayers[areaId];
                if (layer) {
                    map.fitBounds(layer.getBounds());
                }
            }
        });

        document.getElementById('ask-btn').addEventListener('click', askQuestion);

        document.getElementById('question-input').addEventListener('keypress', function(e) {
            if (e.key === 'Enter') {
                askQuestion();
            }
        });
    }

    // Initialize on DOM ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
