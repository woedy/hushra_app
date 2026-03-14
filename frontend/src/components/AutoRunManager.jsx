import { useCallback, useEffect, useMemo, useState } from 'react';

const API = import.meta.env.VITE_API_URL || 'http://localhost:8000';
const ALL_STATES = ["AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"];

function apiFetch(path, opts = {}) {
  return fetch(`${API}${path}`, {
    headers: { 'Content-Type': 'application/json', ...(opts.headers || {}) },
    ...opts,
  });
}

function Badge({ status }) {
  const map = {
    RUNNING: 'bg-emerald-500/15 text-emerald-300 border-emerald-500/30',
    PAUSED: 'bg-amber-500/15 text-amber-300 border-amber-500/30',
    COMPLETED: 'bg-blue-500/15 text-blue-300 border-blue-500/30',
    FAILED: 'bg-red-500/15 text-red-300 border-red-500/30',
    PENDING: 'bg-gray-600/20 text-gray-300 border-gray-600/40',
  };


  return (
    <span className={`text-[10px] px-2 py-0.5 rounded-full border font-semibold ${map[status] || map.PENDING}`}>
      {status}
    </span>
  );
}

export default function AutoRunManager() {
  const [status, setStatus] = useState(null);
  const [stateRuns, setStateRuns] = useState([]);
  const [credentials, setCredentials] = useState([]);
  const [autoStates, setAutoStates] = useState([]);
  const [autoAxes, setAutoAxes] = useState(['lastname']);
  const [autoQueueMin, setAutoQueueMin] = useState(200);
  const [lastMsg, setLastMsg] = useState('');
  const [busy, setBusy] = useState(false);
  const [stateRunBusy, setStateRunBusy] = useState(null);
  const [bulkActionBusy, setBulkActionBusy] = useState(false);
  const [selectedStateRuns, setSelectedStateRuns] = useState([]);
  const [uuidBlob, setUuidBlob] = useState('');
  const [recordsPanel, setRecordsPanel] = useState({ open: false, loading: false, stateRun: null, page: 1, totalPages: 1, count: 0, rows: [] });

  const activeUuidCount = useMemo(() => credentials.filter(c => c.is_active).length, [credentials]);


  const hasLoadedStatus = status !== null;
  const credentialsReady = status?.credentials_ready ?? activeUuidCount > 0;

  const loadSettings = useCallback(async () => {
    const r = await apiFetch('/api/settings/');
    if (!r.ok) return;
    const data = await r.json();
    const rows = data.results || data;
    rows.forEach(item => {
      if (item.key === 'auto_queue_min') setAutoQueueMin(parseInt(item.value, 10) || 200);
      if (item.key === 'auto_run_states') setAutoStates(item.value ? item.value.split(',').map(s => s.trim().toUpperCase()).filter(Boolean) : []);
      if (item.key === 'auto_run_axes') setAutoAxes(item.value ? item.value.split(',').map(s => s.trim()).filter(Boolean) : ['lastname']);
    });
  }, []);

  const loadStatus = useCallback(async () => {
    const r = await apiFetch('/api/settings/orchestrator_status/');
    if (!r.ok) return;
    const data = await r.json();
    setStatus(data);
    setAutoQueueMin(data.min_queue ?? 200);
  }, []);

  const loadStateRuns = useCallback(async () => {
    const r = await apiFetch('/api/state-runs/?ordering=-updated_at');
    if (!r.ok) return;
    const data = await r.json();
    setStateRuns(data.results || data);
  }, []);

  const loadCredentials = useCallback(async () => {
    const r = await apiFetch('/api/credentials/');
    if (!r.ok) return;
    const data = await r.json();
    setCredentials(data.results || data);
  }, []);

  const refreshLive = useCallback(async () => {
    await Promise.all([loadStatus(), loadStateRuns(), loadCredentials()]);
  }, [loadCredentials, loadStateRuns, loadStatus]);

  const refreshAll = useCallback(async () => {
    await Promise.all([loadStatus(), loadStateRuns(), loadCredentials(), loadSettings()]);
  }, [loadCredentials, loadSettings, loadStateRuns, loadStatus]);

  useEffect(() => {
    refreshAll();
    const id = setInterval(refreshLive, 5000);
    return () => clearInterval(id);
  }, [refreshAll, refreshLive]);


  const sortedSelectedStates = useMemo(() => [...autoStates].sort(), [autoStates]);

  const metrics = useMemo(() => {
    const totals = stateRuns.reduce((acc, sr) => {
      acc.totalRuns += 1;
      acc.pending += sr.tasks_pending || 0;
      acc.inProgress += sr.tasks_in_progress || 0;
      acc.completed += sr.tasks_completed || 0;
      acc.failed += sr.tasks_failed || 0;
      acc.records += sr.total_records || 0;
      acc.primesDone += sr.primes_completed || 0;
      acc.primesTotal += sr.total_primes || 0;
      acc.tpm += sr.tasks_per_minute || 0;
      acc.rpt += sr.records_per_task || 0;
      return acc;
    }, { totalRuns: 0, pending: 0, inProgress: 0, completed: 0, failed: 0, records: 0, primesDone: 0, primesTotal: 0, tpm: 0, rpt: 0 });

    const avgTpm = totals.totalRuns ? totals.tpm / totals.totalRuns : 0;
    const avgRpt = totals.totalRuns ? totals.rpt / totals.totalRuns : 0;
    const primeProgress = totals.primesTotal ? (totals.primesDone / totals.primesTotal) * 100 : 0;

    return { ...totals, avgTpm, avgRpt, primeProgress };
  }, [stateRuns]);

  const persistConfig = useCallback(async () => {
    await apiFetch('/api/settings/set_value/', { method: 'POST', body: JSON.stringify({ key: 'auto_queue_min', value: autoQueueMin }) });
    await apiFetch('/api/settings/set_value/', { method: 'POST', body: JSON.stringify({ key: 'auto_run_states', value: autoStates.join(',') }) });
    await apiFetch('/api/settings/set_value/', { method: 'POST', body: JSON.stringify({ key: 'auto_run_axes', value: autoAxes.join(',') }) });
  }, [autoAxes, autoQueueMin, autoStates]);

  const saveConfig = async () => {
    setBusy(true);
    setLastMsg('');
    try {
      await persistConfig();
      setLastMsg('Configuration saved.');
      await refreshAll();
    } finally {
      setBusy(false);
    }
  };

  const applyFastestSafePreset = async () => {
    setBusy(true);
    setLastMsg('');
    try {
      const nextStates = ALL_STATES;
      const nextAxes = ['lastname'];
      const nextQueueMin = 200;

      setAutoStates(nextStates);
      setAutoAxes(nextAxes);
      setAutoQueueMin(nextQueueMin);

      await apiFetch('/api/settings/set_value/', { method: 'POST', body: JSON.stringify({ key: 'auto_queue_min', value: nextQueueMin }) });
      await apiFetch('/api/settings/set_value/', { method: 'POST', body: JSON.stringify({ key: 'auto_run_states', value: nextStates.join(',') }) });
      await apiFetch('/api/settings/set_value/', { method: 'POST', body: JSON.stringify({ key: 'auto_run_axes', value: nextAxes.join(',') }) });
      await apiFetch('/api/settings/set_value/', { method: 'POST', body: JSON.stringify({ key: 'auto_state_order_mode', value: 'round_robin' }) });
      await apiFetch('/api/settings/set_value/', { method: 'POST', body: JSON.stringify({ key: 'soft_limit', value: 100 }) });
      await apiFetch('/api/settings/set_value/', { method: 'POST', body: JSON.stringify({ key: 'use_proxy', value: false }) });

      setLastMsg('Applied Fastest Safe preset: all states, lastname axis, round-robin state order, queue min 200, soft limit 100, proxy off.');
      await refreshAll();
    } finally {
      setBusy(false);
    }
  };

  const toggleAutoRun = async () => {
    if (!credentialsReady) {
      setLastMsg('Add at least one active UUID credential before starting Auto Run.');
      return;
    }

    setBusy(true);
    setLastMsg('');
    try {
      await persistConfig();
      const r = await apiFetch('/api/settings/toggle/', { method: 'POST', body: JSON.stringify({ key: 'auto_run_enabled' }) });
      const d = await r.json();
      const nowEnabled = d.value === true || d.value === 'true';
      if (nowEnabled) {
        const seed = await apiFetch('/api/settings/seed_now/', { method: 'POST' });
        const payload = await seed.json();
        setLastMsg(seed.ok ? payload.message : payload.error || 'Unable to seed now.');
      }
      await refreshAll();
    } finally {
      setBusy(false);
    }
  };

  const seedNow = async () => {
    if (!credentialsReady) {
      setLastMsg('Add at least one active UUID credential before seeding.');
      return;
    }
    setBusy(true);
    try {
      await persistConfig();
      const r = await apiFetch('/api/settings/seed_now/', { method: 'POST' });
      const d = await r.json();
      setLastMsg(r.ok ? d.message : (d.error || 'Seed failed.'));
      await refreshAll();
    } finally {
      setBusy(false);
    }
  };

  const resetSession = async () => {
    if (!window.confirm('Start a new session? This clears completed/stopped/failed tasks for auto-run.')) return;
    setBusy(true);
    try {
      const r = await apiFetch('/api/settings/new_session/', { method: 'POST' });
      const d = await r.json();
      setLastMsg(d.message || 'Session reset complete.');
      await refreshAll();
    } finally {
      setBusy(false);
    }
  };

  const runStateAction = async (id, action) => {
    setStateRunBusy(id);
    try {
      const r = await apiFetch(`/api/state-runs/${id}/${action}/`, { method: 'POST' });
      const d = await r.json();
      setLastMsg(d.message || d.error || `State action ${action} complete.`);
      await refreshAll();
    } finally {
      setStateRunBusy(null);
    }
  };

  const toggleStateRunSelection = (id) => {
    setSelectedStateRuns(prev => prev.includes(id) ? prev.filter(x => x !== id) : [...prev, id]);
  };

  const bulkStateAction = async (action) => {
    if (selectedStateRuns.length === 0) {
      setLastMsg('Select at least one state run first.');
      return;
    }
    setBulkActionBusy(true);
    try {
      const r = await apiFetch('/api/state-runs/bulk_control/', {
        method: 'POST',
        body: JSON.stringify({ action, state_run_ids: selectedStateRuns }),
      });
      const d = await r.json();
      setLastMsg(d.message || d.error || `Bulk ${action} complete.`);
      if (r.ok) setSelectedStateRuns([]);
      await refreshAll();
    } finally {
      setBulkActionBusy(false);
    }
  };

  const closeRecordsPanel = () => {
    setRecordsPanel({ open: false, loading: false, stateRun: null, page: 1, totalPages: 1, count: 0, rows: [] });
  };

  const openStateRunRecords = async (stateRun, page = 1) => {
    setRecordsPanel(prev => ({ ...prev, open: true, loading: true, stateRun }));
    try {
      const r = await apiFetch(`/api/state-runs/${stateRun.id}/records/?page=${page}&page_size=25`);
      const d = await r.json();
      if (!r.ok) {
        setLastMsg(d.error || 'Unable to load records for this state run.');
        setRecordsPanel(prev => ({ ...prev, loading: false }));
        return;
      }
      setRecordsPanel({
        open: true,
        loading: false,
        stateRun,
        page: d.page || 1,
        totalPages: d.total_pages || 1,
        count: d.count || 0,
        rows: d.results || [],
      });
    } finally {
      setRecordsPanel(prev => ({ ...prev, loading: false }));
    }
  };

  const addUuids = async () => {
    if (!uuidBlob.trim()) return;
    setBusy(true);
    try {
      const r = await apiFetch('/api/credentials/bulk_add/', {
        method: 'POST',
        body: JSON.stringify({ uuids: uuidBlob }),
      });
      const d = await r.json();
      setLastMsg(d.message || d.error || 'UUID update complete.');
      if (r.ok) setUuidBlob('');
      await refreshAll();
    } finally {
      setBusy(false);
    }
  };


  const resetUuidPool = async () => {
    setBusy(true);
    try {
      const r = await apiFetch('/api/credentials/reset_pool/', { method: 'POST' });
      const d = await r.json();
      setLastMsg(d.message || d.error || 'UUID pool reset complete.');
      await refreshAll();
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="space-y-6">
      {hasLoadedStatus && !credentialsReady && (
        <div className="bg-red-500/10 border border-red-500/40 rounded-xl p-4">
          <p className="text-sm font-bold text-red-300">Auto Run is blocked</p>
          <p className="text-xs text-red-200/90 mt-1">You need at least one active UUID credential before Auto Run tasks can start. If UUIDs are exhausted, use Reset UUID Pool below.</p>
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <div className="bg-gray-800 border border-gray-700 rounded-xl p-4 space-y-3 lg:col-span-2">
          <div className="flex items-center justify-between">
            <h2 className="font-bold text-blue-300">Auto Run Control Center</h2>
            <button onClick={refreshAll} className="text-xs text-blue-400 hover:text-blue-300">⟳ Refresh</button>
          </div>

          <div className="grid grid-cols-2 md:grid-cols-4 gap-2">
            <div className="bg-gray-900 border border-gray-700 rounded p-2 text-center"><p className="text-lg font-bold text-yellow-300">{status?.pending ?? 0}</p><p className="text-[10px] text-gray-500">Pending</p></div>
            <div className="bg-gray-900 border border-gray-700 rounded p-2 text-center"><p className="text-lg font-bold text-blue-300">{status?.in_progress ?? 0}</p><p className="text-[10px] text-gray-500">Running</p></div>
            <div className="bg-gray-900 border border-gray-700 rounded p-2 text-center"><p className="text-lg font-bold text-emerald-300">{status?.completed ?? 0}</p><p className="text-[10px] text-gray-500">Done</p></div>
            <div className="bg-gray-900 border border-gray-700 rounded p-2 text-center"><p className="text-lg font-bold text-red-300">{status?.failed ?? 0}</p><p className="text-[10px] text-gray-500">Failed</p></div>
          </div>

          <div className="flex flex-wrap gap-2">
            <button onClick={toggleAutoRun} disabled={busy || !credentialsReady || autoStates.length === 0}
              className="bg-emerald-600 hover:bg-emerald-500 disabled:opacity-40 text-white px-4 py-2 rounded-lg text-sm font-semibold">
              {status?.enabled ? 'Stop Auto Run' : 'Start Auto Run'}
            </button>
            <button onClick={seedNow} disabled={busy || !credentialsReady || autoStates.length === 0}
              className="bg-blue-600 hover:bg-blue-500 disabled:opacity-40 text-white px-4 py-2 rounded-lg text-sm font-semibold">Seed Now</button>
            <button onClick={saveConfig} disabled={busy}
              className="bg-indigo-600 hover:bg-indigo-500 disabled:opacity-40 text-white px-4 py-2 rounded-lg text-sm font-semibold">Save Config</button>
            <button onClick={applyFastestSafePreset} disabled={busy}
              className="bg-cyan-700 hover:bg-cyan-600 disabled:opacity-40 text-white px-4 py-2 rounded-lg text-sm font-semibold">Apply Fastest Safe</button>
            <button onClick={resetSession} disabled={busy}
              className="bg-gray-700 hover:bg-gray-600 disabled:opacity-40 text-white px-4 py-2 rounded-lg text-sm font-semibold">New Session Reset</button>
          </div>

          <div className="grid md:grid-cols-2 gap-3">
            <div>
              <label className="text-xs text-gray-400 font-semibold">Queue Min Target</label>
              <input type="number" value={autoQueueMin} onChange={e => setAutoQueueMin(e.target.value)}
                className="w-full mt-1 bg-gray-900 border border-gray-700 rounded p-2 text-sm" />
            </div>
            <div>
              <label className="text-xs text-gray-400 font-semibold">Axes</label>
              <div className="mt-1 flex gap-2">
                {['lastname', 'firstname', 'city'].map(ax => (
                  <button key={ax} onClick={() => setAutoAxes(prev => prev.includes(ax) ? prev.filter(x => x !== ax) : [...prev, ax])}
                    className={`px-3 py-1 text-xs rounded border capitalize ${autoAxes.includes(ax) ? 'bg-blue-500/20 border-blue-500 text-blue-300' : 'border-gray-700 text-gray-400'}`}>
                    {ax}
                  </button>
                ))}
              </div>
            </div>
          </div>

          <div>
            <div className="mb-2">
              <p className="text-xs text-gray-400 font-semibold mb-1">Selected States</p>
              {sortedSelectedStates.length === 0 ? (
                <p className="text-xs text-gray-500">No states selected yet.</p>
              ) : (
                <div className="flex flex-wrap gap-1.5">
                  {sortedSelectedStates.map(st => (
                    <button
                      key={`chip-${st}`}
                      onClick={() => setAutoStates(prev => prev.filter(s => s !== st))}
                      className="text-[10px] px-2 py-1 rounded-full bg-blue-500/20 border border-blue-500 text-blue-200 hover:bg-red-500/20 hover:border-red-500 hover:text-red-200 transition-colors"
                      title="Click to remove"
                    >
                      {st} ×
                    </button>
                  ))}
                </div>
              )}
            </div>

            <div className="flex justify-between items-center mb-1">
              <label className="text-xs text-gray-400 font-semibold">Target States ({autoStates.length})</label>
              <div className="text-[10px] flex gap-2">
                <button className="text-blue-400" onClick={() => setAutoStates(ALL_STATES)}>All</button>
                <button className="text-red-400" onClick={() => setAutoStates([])}>Clear</button>
              </div>
            </div>
            <div className="grid grid-cols-10 gap-1 max-h-28 overflow-y-auto">
              {ALL_STATES.map(st => (
                <button key={st} onClick={() => setAutoStates(prev => prev.includes(st) ? prev.filter(s => s !== st) : [...prev, st])}
                  className={`text-[10px] py-1 rounded border font-semibold transition-colors ${autoStates.includes(st) ? 'bg-blue-500/35 border-blue-400 text-blue-100' : 'bg-gray-900 border-gray-700 text-gray-500 hover:border-gray-500 hover:text-gray-300'}`}>
                  {st}
                </button>
              ))}
            </div>
          </div>

          {lastMsg && <p className="text-xs text-gray-300 bg-gray-900 border border-gray-700 rounded p-2">{lastMsg}</p>}
        </div>

        <div className="bg-gray-800 border border-gray-700 rounded-xl p-4 space-y-3">
          <h3 className="font-semibold text-amber-300">UUID Credential Gate</h3>
          <div className="text-xs text-gray-300 space-y-1">
            <p>Total UUIDs: <span className="font-bold">{status?.total_credentials ?? credentials.length}</span></p>
            <p>Active UUIDs: <span className="font-bold">{status?.active_credentials ?? activeUuidCount}</span></p>
            <p>Active Available UUIDs: <span className="font-bold">{status?.usable_credentials ?? 0}</span></p>
          </div>
          <textarea value={uuidBlob} onChange={e => setUuidBlob(e.target.value)}
            className="w-full h-28 bg-gray-900 border border-gray-700 rounded p-2 text-xs"
            placeholder="Paste UUIDs, one per line" />
          <button onClick={addUuids} disabled={busy || !uuidBlob.trim()}
            className="w-full bg-amber-600 hover:bg-amber-500 disabled:opacity-40 text-black font-bold py-2 rounded">Add UUIDs</button>
          <button onClick={resetUuidPool} disabled={busy}
            className="w-full bg-gray-700 hover:bg-gray-600 disabled:opacity-40 text-white font-semibold py-2 rounded text-sm">Reset UUID Pool</button>
        </div>
      </div>

      <div className="bg-gray-800 border border-gray-700 rounded-xl p-4 space-y-3">
        <div className="flex items-center justify-between">
          <h3 className="font-semibold text-cyan-300">Auto Run Metrics</h3>
          <p className="text-xs text-gray-500">Live overview across all state runs.</p>
        </div>

        <div className="grid grid-cols-2 md:grid-cols-4 xl:grid-cols-8 gap-2">
          <div className="bg-gray-900 border border-gray-700 rounded p-2 text-center"><p className="text-sm font-bold text-gray-100">{metrics.totalRuns}</p><p className="text-[10px] text-gray-500">Runs</p></div>
          <div className="bg-gray-900 border border-gray-700 rounded p-2 text-center"><p className="text-sm font-bold text-yellow-300">{metrics.pending}</p><p className="text-[10px] text-gray-500">Pending</p></div>
          <div className="bg-gray-900 border border-gray-700 rounded p-2 text-center"><p className="text-sm font-bold text-blue-300">{metrics.inProgress}</p><p className="text-[10px] text-gray-500">In Progress</p></div>
          <div className="bg-gray-900 border border-gray-700 rounded p-2 text-center"><p className="text-sm font-bold text-emerald-300">{metrics.completed}</p><p className="text-[10px] text-gray-500">Completed</p></div>
          <div className="bg-gray-900 border border-gray-700 rounded p-2 text-center"><p className="text-sm font-bold text-red-300">{metrics.failed}</p><p className="text-[10px] text-gray-500">Failed</p></div>
          <div className="bg-gray-900 border border-gray-700 rounded p-2 text-center"><p className="text-sm font-bold text-purple-300">{metrics.records}</p><p className="text-[10px] text-gray-500">Records</p></div>
          <div className="bg-gray-900 border border-gray-700 rounded p-2 text-center"><p className="text-sm font-bold text-cyan-300">{metrics.avgTpm.toFixed(2)}</p><p className="text-[10px] text-gray-500">Avg Tasks/Min</p></div>
          <div className="bg-gray-900 border border-gray-700 rounded p-2 text-center"><p className="text-sm font-bold text-indigo-300">{metrics.avgRpt.toFixed(2)}</p><p className="text-[10px] text-gray-500">Avg Rec/Task</p></div>
        </div>

        <div>
          <div className="flex justify-between text-[11px] text-gray-400 mb-1">
            <span>Prime sweep progress</span>
            <span>{metrics.primesDone}/{metrics.primesTotal || 0} ({Math.round(metrics.primeProgress)}%)</span>
          </div>
          <div className="w-full h-2 rounded-full bg-gray-900 overflow-hidden">
            <div className="h-full bg-cyan-500" style={{ width: `${Math.min(metrics.primeProgress, 100)}%` }} />
          </div>
        </div>
      </div>


      <div className="bg-gray-800 border border-gray-700 rounded-xl p-4">
        <div className="flex items-center justify-between mb-2">
          <h3 className="font-semibold text-gray-100">Per-State Runs</h3>
          <p className="text-xs text-gray-500">Monitor + control each selected state independently.</p>
        </div>
        <div className="flex flex-wrap gap-2 mb-3">
          <button disabled={bulkActionBusy || selectedStateRuns.length === 0} onClick={() => bulkStateAction('pause')}
            className="text-xs px-3 py-1.5 rounded border border-amber-500/40 text-amber-300 disabled:opacity-40">Pause Selected</button>
          <button disabled={bulkActionBusy || selectedStateRuns.length === 0} onClick={() => bulkStateAction('resume')}
            className="text-xs px-3 py-1.5 rounded border border-blue-500/40 text-blue-300 disabled:opacity-40">Resume Selected</button>
          <button disabled={bulkActionBusy || selectedStateRuns.length === 0} onClick={() => bulkStateAction('stop')}
            className="text-xs px-3 py-1.5 rounded border border-red-500/40 text-red-300 disabled:opacity-40">Stop Selected</button>
          <button disabled={bulkActionBusy || selectedStateRuns.length === 0} onClick={() => setSelectedStateRuns([])}
            className="text-xs px-3 py-1.5 rounded border border-gray-600 text-gray-300 disabled:opacity-40">Clear Selection</button>
          <p className="text-xs text-gray-500 self-center">Selected: {selectedStateRuns.length}</p>
        </div>
        {stateRuns.length === 0 ? (
          <p className="text-sm text-gray-500">No state runs yet.</p>
        ) : (
          <div className="grid md:grid-cols-2 xl:grid-cols-3 gap-3">
            {stateRuns.map(sr => {
              const progress = sr.total_primes > 0 ? Math.min((sr.primes_completed / sr.total_primes) * 100, 100) : 0;
              const busyRow = stateRunBusy === sr.id;
              const selected = selectedStateRuns.includes(sr.id);

              return (
                <div key={sr.id} className={`bg-gray-900 border rounded-lg p-3 space-y-2 ${selected ? 'border-blue-400' : 'border-gray-700'}`}>
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-2">
                      <input type="checkbox" checked={selected} onChange={() => toggleStateRunSelection(sr.id)} className="accent-blue-500" />
                      <span className="font-mono font-bold">{sr.state}</span>
                      <Badge status={sr.status} />
                    </div>
                    <span className="text-[10px] text-gray-500">{sr.tasks_completed}/{sr.total_tasks || 0} tasks</span>
                  </div>
                  <div className="w-full h-2 rounded-full bg-gray-800 overflow-hidden"><div className="h-full bg-blue-500" style={{ width: `${progress}%` }} /></div>
                  <div className="flex justify-between text-[10px] text-gray-500"><span>Primes {sr.primes_completed}/{sr.total_primes || 0}</span><span>{Math.round(progress)}%</span></div>
                  <div className="flex gap-1">
                    <button disabled={busyRow || sr.status !== 'RUNNING'} onClick={() => runStateAction(sr.id, 'pause')} className="text-[10px] px-2 py-1 border border-amber-500/30 text-amber-300 rounded disabled:opacity-40">Pause</button>
                    <button disabled={busyRow || sr.status !== 'PAUSED'} onClick={() => runStateAction(sr.id, 'resume')} className="text-[10px] px-2 py-1 border border-blue-500/30 text-blue-300 rounded disabled:opacity-40">Resume</button>
                    <button disabled={busyRow || ['COMPLETED', 'FAILED'].includes(sr.status)} onClick={() => runStateAction(sr.id, 'stop')} className="text-[10px] px-2 py-1 border border-red-500/30 text-red-300 rounded disabled:opacity-40">Stop</button>
                    <button disabled={busyRow} onClick={() => openStateRunRecords(sr, 1)} className="text-[10px] px-2 py-1 border border-cyan-500/30 text-cyan-300 rounded disabled:opacity-40">View Data</button>
                    <button disabled={busyRow} onClick={() => runStateAction(sr.id, 'refresh_metrics')} className="text-[10px] px-2 py-1 border border-gray-600 text-gray-300 rounded ml-auto disabled:opacity-40">Refresh</button>
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </div>

      {recordsPanel.open && (
        <div className="fixed inset-0 z-50 bg-black/70 backdrop-blur-sm flex items-center justify-center p-4">
          <div className="w-full max-w-6xl bg-gray-900 border border-gray-700 rounded-xl shadow-xl">
            <div className="flex items-center justify-between p-4 border-b border-gray-700">
              <div>
                <h3 className="text-lg font-bold text-cyan-300">State {recordsPanel.stateRun?.state} Returned Data</h3>
                <p className="text-xs text-gray-400">Total records: {recordsPanel.count} • Page {recordsPanel.page}/{recordsPanel.totalPages}</p>
              </div>
              <button onClick={closeRecordsPanel} className="text-sm px-3 py-1.5 rounded bg-gray-700 hover:bg-gray-600">Close</button>
            </div>

            <div className="p-4 max-h-[70vh] overflow-auto">
              {recordsPanel.loading ? (
                <p className="text-sm text-gray-400">Loading records...</p>
              ) : recordsPanel.rows.length === 0 ? (
                <p className="text-sm text-gray-400">No records found for this state run yet.</p>
              ) : (
                <table className="min-w-full text-xs">
                  <thead>
                    <tr className="text-left text-gray-400 border-b border-gray-700">
                      <th className="py-2 pr-3">Name</th>
                      <th className="py-2 pr-3">SSN</th>
                      <th className="py-2 pr-3">DOB</th>
                      <th className="py-2 pr-3">Address</th>
                      <th className="py-2 pr-3">City</th>
                      <th className="py-2 pr-3">State</th>
                      <th className="py-2 pr-3">ZIP</th>
                    </tr>
                  </thead>
                  <tbody>
                    {recordsPanel.rows.map((row) => (
                      <tr key={row.id} className="border-b border-gray-800 text-gray-200">
                        <td className="py-2 pr-3">{[row.firstname, row.middlename, row.lastname].filter(Boolean).join(' ') || '—'}</td>
                        <td className="py-2 pr-3">{row.ssn || '—'}</td>
                        <td className="py-2 pr-3">{row.dob || '—'}</td>
                        <td className="py-2 pr-3">{row.address || '—'}</td>
                        <td className="py-2 pr-3">{row.city || '—'}</td>
                        <td className="py-2 pr-3">{row.state || '—'}</td>
                        <td className="py-2 pr-3">{row.zip_code || '—'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>

            <div className="p-4 border-t border-gray-700 flex items-center justify-between">
              <button
                disabled={recordsPanel.loading || recordsPanel.page <= 1}
                onClick={() => openStateRunRecords(recordsPanel.stateRun, recordsPanel.page - 1)}
                className="px-3 py-1.5 rounded bg-gray-700 hover:bg-gray-600 disabled:opacity-40 text-sm"
              >Prev</button>
              <button
                disabled={recordsPanel.loading || recordsPanel.page >= recordsPanel.totalPages}
                onClick={() => openStateRunRecords(recordsPanel.stateRun, recordsPanel.page + 1)}
                className="px-3 py-1.5 rounded bg-gray-700 hover:bg-gray-600 disabled:opacity-40 text-sm"
              >Next</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
