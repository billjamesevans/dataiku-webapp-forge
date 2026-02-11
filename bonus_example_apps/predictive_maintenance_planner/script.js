/* globals dataiku, getWebAppBackendUrl */
(function () {
  'use strict';

  var state = {
    days: 14,
    selectedMachine: '',
    dashboard: null,
  };

  function backendUrl(path) {
    var clean = path || '';

    if (typeof getWebAppBackendUrl === 'function') {
      try {
        var direct = getWebAppBackendUrl(clean);
        if (direct) return direct;
      } catch (e) {}
    }

    if (window.dataiku && typeof dataiku.getWebAppBackendUrl === 'function') {
      try {
        var modern = dataiku.getWebAppBackendUrl(clean);
        if (modern) return modern;
      } catch (e2) {}
    }

    if (window.dataiku && typeof dataiku.getWbAppBackendUrl === 'function') {
      try {
        var legacy = dataiku.getWbAppBackendUrl(clean);
        if (legacy) return legacy;
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

  function fmtMoney(n) {
    return '$' + Number(n || 0).toLocaleString(undefined, { maximumFractionDigits: 0 });
  }

  function writeKpis(summary) {
    var cards = [
      { label: 'Machines High Risk', value: fmtInt(summary.machines_high_risk), meta: 'risk score >= 75' },
      { label: 'Predicted Failures (30d)', value: fmtInt(summary.predicted_failures_30d), meta: 'derived from RUL + anomaly profile' },
      { label: 'Planned Labor Load', value: fmtInt(summary.scheduled_hours_14d) + 'h', meta: 'next 14 days' },
      { label: 'At-Risk Parts Value', value: fmtMoney(summary.parts_exposure_value), meta: 'critical shortage candidates' },
      { label: 'Projected Savings', value: fmtMoney(summary.projected_savings_usd), meta: 'if recommendations executed' },
      { label: 'Backlog SLA Risk', value: fmtPct(summary.backlog_sla_risk_pct), meta: 'work orders due in <= 7 days' },
    ];

    document.getElementById('kpiCards').innerHTML = cards
      .map(function (k) {
        return [
          '<article class="kpi">',
          '<span class="label">' + k.label + '</span>',
          '<strong class="value">' + k.value + '</strong>',
          '<span class="meta">' + k.meta + '</span>',
          '</article>',
        ].join('');
      })
      .join('');
  }

  function renderRiskBoard(rows) {
    var node = document.getElementById('riskBoard');
    if (!rows || !rows.length) {
      node.innerHTML = '<p>No risk scoring data.</p>';
      return;
    }

    node.innerHTML = rows
      .map(function (row) {
        return [
          '<div class="risk-row">',
          '<span>' + row.machine_id + ' (' + row.line + ')</span>',
          '<div class="risk-track"><span style="width:' + Math.max(4, Math.min(100, row.risk_score)).toFixed(1) + '%"></span></div>',
          '<strong>' + Math.round(row.risk_score) + '</strong>',
          '</div>',
        ].join('');
      })
      .join('');
  }

  function renderSchedule(schedule) {
    var badge = document.getElementById('queueBadge');
    var root = document.getElementById('scheduleBoard');
    var lanes = [
      { key: 'now', label: 'Now' },
      { key: 'week', label: 'This Week' },
      { key: 'next', label: 'Next Week' },
    ];

    var grouped = { now: [], week: [], next: [] };
    (schedule || []).forEach(function (item) {
      grouped[item.bucket] = grouped[item.bucket] || [];
      grouped[item.bucket].push(item);
    });

    badge.textContent = (schedule || []).length + ' items';

    root.innerHTML = lanes
      .map(function (lane) {
        var items = grouped[lane.key] || [];
        return [
          '<section class="lane">',
          '<h3>' + lane.label + '</h3>',
          '<div class="lane-items">',
          items
            .map(function (it) {
              return [
                '<article class="task">',
                '<div class="top"><span>' + it.machine_id + '</span><span>' + it.priority + '</span></div>',
                '<div class="meta">' + it.action + '</div>',
                '<div class="meta">Due ' + it.target_date + ' | ' + it.team + '</div>',
                '</article>',
              ].join('');
            })
            .join('') || '<div class="meta">No tasks</div>',
          '</div>',
          '</section>',
        ].join('');
      })
      .join('');
  }

  function renderParts(parts) {
    var body = document.getElementById('partsBody');
    if (!parts || !parts.length) {
      body.innerHTML = '<tr><td colspan="7">No parts exposure found.</td></tr>';
      return;
    }

    body.innerHTML = parts
      .map(function (row) {
        return [
          '<tr>',
          '<td>' + row.part_name + '</td>',
          '<td>' + row.machine_family + '</td>',
          '<td>' + fmtInt(row.on_hand) + '</td>',
          '<td>' + fmtInt(row.reorder_point) + '</td>',
          '<td>' + fmtInt(row.lead_time_days) + '</td>',
          '<td>' + fmtInt(row.coverage_days) + 'd</td>',
          '<td><span class="badge ' + row.risk_level.toLowerCase() + '">' + row.risk_level + '</span></td>',
          '</tr>',
        ].join('');
      })
      .join('');
  }

  function renderWorkOrders(orders) {
    var body = document.getElementById('workOrdersBody');
    if (!orders || !orders.length) {
      body.innerHTML = '<tr><td colspan="8">No work orders loaded.</td></tr>';
      return;
    }

    body.innerHTML = orders
      .map(function (row) {
        return [
          '<tr>',
          '<td>' + row.wo_id + '</td>',
          '<td>' + row.machine_id + '</td>',
          '<td><span class="badge ' + row.priority.toLowerCase() + '">' + row.priority + '</span></td>',
          '<td>' + row.status + '</td>',
          '<td>' + row.due_date + '</td>',
          '<td>' + fmtInt(row.estimated_hours) + '</td>',
          '<td>' + row.failure_mode + '</td>',
          '<td>' + row.assigned_team + '</td>',
          '</tr>',
        ].join('');
      })
      .join('');
  }

  function updateMachineSelect(riskRows) {
    var select = document.getElementById('machineSelect');
    var machines = (riskRows || []).map(function (r) {
      return r.machine_id;
    });

    if (!machines.length) {
      select.innerHTML = '<option value="">No machines</option>';
      state.selectedMachine = '';
      return;
    }

    if (!state.selectedMachine || machines.indexOf(state.selectedMachine) < 0) {
      state.selectedMachine = machines[0];
    }

    select.innerHTML = machines
      .map(function (m) {
        return '<option value="' + m + '"' + (m === state.selectedMachine ? ' selected' : '') + '>' + m + '</option>';
      })
      .join('');
  }

  function renderTrend(points) {
    var node = document.getElementById('machineTrend');
    if (!points || !points.length) {
      node.innerHTML = '<p>No trend points for selected machine.</p>';
      return;
    }

    var width = 640;
    var height = 220;
    var left = 40;
    var bottom = 20;
    var top = 10;

    var values = points.map(function (p) {
      return p.risk_score;
    });
    var min = Math.min.apply(Math, values);
    var max = Math.max.apply(Math, values);
    var span = Math.max(1, max - min);

    var path = values
      .map(function (v, idx) {
        var x = left + (idx * (width - left - 12)) / Math.max(1, values.length - 1);
        var y = height - bottom - ((v - min) / span) * (height - bottom - top);
        return (idx === 0 ? 'M ' : ' L ') + x.toFixed(1) + ' ' + y.toFixed(1);
      })
      .join('');

    var labels = points
      .map(function (p, idx) {
        if (idx % Math.ceil(points.length / 5) !== 0 && idx !== points.length - 1) return '';
        var x = left + (idx * (width - left - 12)) / Math.max(1, points.length - 1);
        return '<text x="' + x.toFixed(1) + '" y="212" font-size="11" fill="#64748b">' + p.day.slice(5) + '</text>';
      })
      .join('');

    node.innerHTML = [
      '<svg class="spark" viewBox="0 0 ' + width + ' ' + height + '" preserveAspectRatio="none">',
      '<rect width="' + width + '" height="' + height + '" fill="#fbfdff"></rect>',
      '<path d="' + path + '" stroke="#1e3a8a" stroke-width="3" fill="none"></path>',
      labels,
      '</svg>',
    ].join('');
  }

  function loadDashboard() {
    return fetch(backendUrl('/dashboard?days=' + encodeURIComponent(String(state.days))))
      .then(function (resp) {
        return resp.json();
      })
      .then(function (payload) {
        if (!payload || payload.status !== 'ok') {
          throw new Error(payload && payload.message ? payload.message : 'Dashboard failed');
        }

        state.dashboard = payload;
        writeKpis(payload.summary || {});
        renderRiskBoard(payload.machine_risk || []);
        renderSchedule(payload.schedule || []);
        renderParts(payload.parts_exposure || []);
        renderWorkOrders(payload.open_work_orders || []);
        updateMachineSelect(payload.machine_risk || []);
        document.getElementById('statusStamp').textContent = 'Updated ' + (payload.generated_at || new Date().toISOString());
      });
  }

  function loadMachineTrend() {
    if (!state.selectedMachine) {
      renderTrend([]);
      return Promise.resolve();
    }

    return fetch(
      backendUrl(
        '/machine-trend?machine_id=' + encodeURIComponent(state.selectedMachine) + '&days=' + encodeURIComponent(String(state.days))
      )
    )
      .then(function (resp) {
        return resp.json();
      })
      .then(function (payload) {
        if (!payload || payload.status !== 'ok') {
          throw new Error(payload && payload.message ? payload.message : 'Trend failed');
        }
        renderTrend(payload.points || []);
      });
  }

  function refreshAll() {
    loadDashboard()
      .then(loadMachineTrend)
      .catch(function (err) {
        showToast(err && err.message ? err.message : 'Request failed');
      });
  }

  function bind() {
    document.getElementById('reloadBtn').addEventListener('click', refreshAll);

    document.getElementById('daysInput').addEventListener('change', function (event) {
      state.days = Number(event.target.value || 14);
      refreshAll();
    });

    document.getElementById('machineSelect').addEventListener('change', function (event) {
      state.selectedMachine = event.target.value;
      loadMachineTrend().catch(function (err) {
        showToast(err && err.message ? err.message : 'Trend request failed');
      });
    });
  }

  bind();
  refreshAll();
})();
