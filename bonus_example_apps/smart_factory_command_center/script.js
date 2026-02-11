/* globals dataiku, getWebAppBackendUrl */
(function () {
  'use strict';

  var state = {
    days: 14,
    payload: null,
  };

  function backendUrl(path) {
    var clean = path || '';

    if (typeof getWebAppBackendUrl === 'function') {
      try {
        var resolved = getWebAppBackendUrl(clean);
        if (resolved) return resolved;
      } catch (e) {}
    }

    if (window.dataiku && typeof dataiku.getWebAppBackendUrl === 'function') {
      try {
        var resolvedDataiku = dataiku.getWebAppBackendUrl(clean);
        if (resolvedDataiku) return resolvedDataiku;
      } catch (e2) {}
    }

    if (window.dataiku && typeof dataiku.getWbAppBackendUrl === 'function') {
      try {
        var resolvedLegacy = dataiku.getWbAppBackendUrl(clean);
        if (resolvedLegacy) return resolvedLegacy;
      } catch (e3) {}
    }

    return clean;
  }

  function showToast(message) {
    var node = document.getElementById('toast');
    node.textContent = message;
    node.classList.add('show');
    window.setTimeout(function () {
      node.classList.remove('show');
    }, 2500);
  }

  function fmtInt(n) {
    return Number(n || 0).toLocaleString();
  }

  function fmtPct(n) {
    return Number(n || 0).toFixed(1) + '%';
  }

  function fmtDecimal(n, digits) {
    return Number(n || 0).toFixed(digits || 2);
  }

  function buildKpis(summary) {
    var container = document.getElementById('kpiGrid');
    var cards = [
      { label: 'Overall OEE', value: fmtPct(summary.oee_pct), meta: 'Availability x Performance x Quality' },
      { label: 'Throughput', value: fmtInt(summary.total_good_units), meta: 'Good units shipped' },
      { label: 'Scrap Rate', value: fmtPct(summary.scrap_rate_pct), meta: fmtInt(summary.total_scrap_units) + ' scrap units' },
      { label: 'Downtime', value: fmtInt(summary.total_downtime_minutes) + ' min', meta: fmtDecimal(summary.downtime_hours, 1) + ' hours lost' },
      { label: 'Energy Intensity', value: fmtDecimal(summary.energy_kwh_per_good_unit, 2), meta: 'kWh / good unit' },
      { label: 'OTIF', value: fmtPct(summary.otif_pct), meta: 'Orders shipped on-time / in-full' },
    ];

    container.innerHTML = cards
      .map(function (item) {
        return [
          '<article class="kpi">',
          '<span class="kpi-label">' + item.label + '</span>',
          '<strong class="kpi-value">' + item.value + '</strong>',
          '<span class="kpi-meta">' + item.meta + '</span>',
          '</article>',
        ].join('');
      })
      .join('');
  }

  function makePath(points, width, height, minVal, maxVal, leftPad, bottomPad, topPad) {
    var span = Math.max(1, maxVal - minVal);
    return points
      .map(function (p, idx) {
        var x = leftPad + (idx * (width - leftPad - 10)) / Math.max(1, points.length - 1);
        var y = height - bottomPad - ((p - minVal) / span) * (height - bottomPad - topPad);
        return (idx === 0 ? 'M ' : ' L ') + x.toFixed(1) + ' ' + y.toFixed(1);
      })
      .join('');
  }

  function renderTrend(trend) {
    var node = document.getElementById('trendChart');
    var summary = document.getElementById('trendSummary');
    if (!trend || !trend.length) {
      node.innerHTML = '<p>No trend data found.</p>';
      summary.textContent = 'No data';
      return;
    }

    var width = 760;
    var height = 240;
    var leftPad = 44;
    var bottomPad = 24;
    var topPad = 12;

    var units = trend.map(function (d) {
      return d.good_units;
    });
    var oee = trend.map(function (d) {
      return d.oee_pct;
    });

    var maxUnits = Math.max.apply(Math, units);
    var minUnits = Math.min.apply(Math, units);

    var unitsPath = makePath(units, width, height, minUnits * 0.92, maxUnits * 1.08, leftPad, bottomPad, topPad);
    var oeeScaled = oee.map(function (v) {
      return (v / 100) * maxUnits;
    });
    var oeePath = makePath(oeeScaled, width, height, minUnits * 0.92, maxUnits * 1.08, leftPad, bottomPad, topPad);

    var xTicks = trend
      .map(function (d, idx) {
        if (idx % Math.ceil(trend.length / 7) !== 0 && idx !== trend.length - 1) return '';
        var x = leftPad + (idx * (width - leftPad - 10)) / Math.max(1, trend.length - 1);
        return '<text x="' + x.toFixed(1) + '" y="232" font-size="11" fill="#5f7483">' + d.day.slice(5) + '</text>';
      })
      .join('');

    node.innerHTML = [
      '<div class="spark-wrap">',
      '<svg class="spark-svg" viewBox="0 0 ' + width + ' ' + height + '" preserveAspectRatio="none">',
      '<rect x="0" y="0" width="' + width + '" height="' + height + '" fill="#fbfdfe" />',
      '<path d="' + unitsPath + '" stroke="#0d9488" stroke-width="3" fill="none" />',
      '<path d="' + oeePath + '" stroke="#d97706" stroke-width="2.5" stroke-dasharray="6 4" fill="none" />',
      xTicks,
      '</svg>',
      '<div class="legend">',
      '<span><i style="background:#0d9488"></i>Good units</span>',
      '<span><i style="background:#d97706"></i>OEE (scaled)</span>',
      '</div>',
      '</div>',
    ].join('');

    var delta = trend[trend.length - 1].good_units - trend[0].good_units;
    var dir = delta >= 0 ? 'up' : 'down';
    summary.textContent = 'Throughput ' + dir + ' ' + fmtInt(Math.abs(delta)) + ' units vs window start';
  }

  function renderLineBoard(lines) {
    var node = document.getElementById('lineBoard');
    if (!lines || !lines.length) {
      node.innerHTML = '<p>No line metrics available.</p>';
      return;
    }

    node.innerHTML = lines
      .map(function (line) {
        return [
          '<article class="line-card">',
          '<div class="line-head"><span>' + line.line + '</span><span>OEE ' + fmtPct(line.oee_pct) + '</span></div>',
          '<div class="meter"><span style="width:' + Math.max(4, Math.min(100, line.oee_pct)).toFixed(1) + '%"></span></div>',
          '<div class="metrics-row">',
          '<span>Good: ' + fmtInt(line.good_units) + '</span>',
          '<span>Downtime: ' + fmtInt(line.downtime_minutes) + ' min</span>',
          '<span>Scrap: ' + fmtPct(line.scrap_rate_pct) + '</span>',
          '</div>',
          '</article>',
        ].join('');
      })
      .join('');
  }

  function renderDefects(defects) {
    var node = document.getElementById('defectBars');
    if (!defects || !defects.length) {
      node.innerHTML = '<p>No defect records in this window.</p>';
      return;
    }

    var max = defects.reduce(function (acc, row) {
      return Math.max(acc, row.units_impacted);
    }, 0);

    node.innerHTML = defects
      .map(function (row) {
        var width = ((row.units_impacted / max) * 100).toFixed(1);
        return [
          '<div class="bar-item">',
          '<span>' + row.defect_type + '</span>',
          '<div class="bar-track"><span style="width:' + width + '%"></span></div>',
          '<strong>' + fmtInt(row.units_impacted) + '</strong>',
          '</div>',
        ].join('');
      })
      .join('');
  }

  function renderAlerts(alerts) {
    var node = document.getElementById('alertList');
    var badge = document.getElementById('alertCount');

    if (!alerts || !alerts.length) {
      node.innerHTML = '<p>No active alerts in the latest telemetry slice.</p>';
      badge.textContent = '0 active';
      badge.className = 'chip';
      return;
    }

    badge.textContent = alerts.length + ' active';
    badge.className = 'chip critical';

    node.innerHTML = alerts
      .map(function (alert) {
        return [
          '<article class="alert-item ' + (alert.severity === 'high' ? 'high' : 'medium') + '">',
          '<div class="alert-title">' + alert.machine_id + ' - ' + alert.reason + '</div>',
          '<div class="alert-meta">',
          alert.line + ' | ' + alert.timestamp + ' | temp ' + fmtDecimal(alert.temperature_c, 1) + ' C | vibration ' + fmtDecimal(alert.vibration_mm_s, 2),
          '</div>',
          '</article>',
        ].join('');
      })
      .join('');
  }

  function renderTelemetry(rows) {
    var body = document.getElementById('telemetryBody');
    if (!rows || !rows.length) {
      body.innerHTML = '<tr><td colspan="8">No telemetry available.</td></tr>';
      return;
    }

    body.innerHTML = rows
      .map(function (row) {
        return [
          '<tr>',
          '<td>' + row.timestamp + '</td>',
          '<td>' + row.machine_id + '</td>',
          '<td>' + row.line + '</td>',
          '<td><span class="status ' + row.status + '">' + row.status + '</span></td>',
          '<td>' + fmtDecimal(row.temperature_c, 1) + '</td>',
          '<td>' + fmtDecimal(row.vibration_mm_s, 2) + '</td>',
          '<td>' + fmtDecimal(row.power_kw, 1) + '</td>',
          '<td>' + row.operator + '</td>',
          '</tr>',
        ].join('');
      })
      .join('');
  }

  function loadFeed() {
    return fetch(backendUrl('/live-feed'))
      .then(function (resp) {
        return resp.json();
      })
      .then(function (payload) {
        if (!payload || payload.status !== 'ok') throw new Error(payload && payload.message ? payload.message : 'Failed feed');
        renderTelemetry(payload.rows || []);
      });
  }

  function loadDashboard() {
    var stamp = document.getElementById('lastUpdated');
    var path = backendUrl('/bootstrap?days=' + encodeURIComponent(String(state.days)));

    return fetch(path)
      .then(function (resp) {
        return resp.json();
      })
      .then(function (payload) {
        if (!payload || payload.status !== 'ok') {
          throw new Error(payload && payload.message ? payload.message : 'Failed dashboard load');
        }
        state.payload = payload;
        buildKpis(payload.summary || {});
        renderTrend(payload.trend || []);
        renderLineBoard(payload.line_breakdown || []);
        renderDefects(payload.defect_pareto || []);
        renderAlerts(payload.alerts || []);
        stamp.textContent = 'Updated ' + (payload.generated_at || new Date().toISOString());
      });
  }

  function refreshAll() {
    Promise.all([loadDashboard(), loadFeed()]).catch(function (err) {
      showToast(err && err.message ? err.message : 'Request failed');
    });
  }

  function bind() {
    document.getElementById('windowDays').addEventListener('change', function (event) {
      state.days = Number(event.target.value || 14);
      refreshAll();
    });

    document.getElementById('refreshBtn').addEventListener('click', refreshAll);
  }

  bind();
  refreshAll();
})();
