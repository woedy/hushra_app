import { useState, useEffect, useCallback } from 'react';

const API = import.meta.env.VITE_API_URL || 'http://localhost:8000';

// ─── helpers ───────────────────────────────────────────────────────────────
function apiFetch(path, opts = {}) {
  return fetch(`${API}${path}`, {
    headers: { 'Content-Type': 'application/json', ...(opts.headers || {}) },
    ...opts,
  });
}

function StatusBadge({ status }) {
  const configs = {
    'IN_PROGRESS': { label: 'Running', bg: 'bg-emerald-500/15', text: 'text-emerald-400', border: 'border-emerald-500/30', pulse: true },
    'PENDING': { label: 'Queued', bg: 'bg-blue-500/15', text: 'text-blue-400', border: 'border-blue-500/30', pulse: false },
    'COMPLETED': { label: 'Done', bg: 'bg-gray-500/15', text: 'text-gray-400', border: 'border-gray-500/30', pulse: false },
    'FAILED': { label: 'Failed', bg: 'bg-red-500/15', text: 'text-red-400', border: 'border-red-500/30', pulse: false },
    'STOPPED': { label: 'Stopped', bg: 'bg-orange-500/15', text: 'text-orange-400', border: 'border-orange-500/30', pulse: false },
    'ABORTED': { label: 'Aborted', bg: 'bg-red-900/15', text: 'text-red-300', border: 'border-red-900/30', pulse: false },
    'TOO_BROAD': { label: 'Too Broad', bg: 'bg-purple-500/15', text: 'text-purple-400', border: 'border-purple-500/30', pulse: false },
    // Account active/inactive labels
    'Active': { label: 'Active', bg: 'bg-emerald-500/15', text: 'text-emerald-400', border: 'border-emerald-500/30', pulse: true },
    'Exhausted': { label: 'Exhausted', bg: 'bg-red-500/15', text: 'text-red-400', border: 'border-red-500/30', pulse: false },
    'On': { label: 'On', bg: 'bg-emerald-500/15', text: 'text-emerald-400', border: 'border-emerald-500/30', pulse: true },
    'Off': { label: 'Off', bg: 'bg-gray-600/15', text: 'text-gray-400', border: 'border-gray-500/30', pulse: false },
  };

  const cfg = configs[status] || { label: status, bg: 'bg-gray-500/15', text: 'text-gray-400', border: 'border-gray-500/30', pulse: false };

  return (
    <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-semibold ${cfg.bg} ${cfg.text} border ${cfg.border}`}>
      {cfg.pulse && <span className={`w-1.5 h-1.5 rounded-full bg-current animate-pulse`} />}
      {cfg.label}
    </span>
  );
}

function StatCard({ title, value, icon, sub }) {
  return (
    <div className="bg-gray-800 border border-gray-700 p-5 rounded-xl shadow-sm flex items-center space-x-4 hover:border-gray-600 transition-colors">
      <div className="text-3xl">{icon}</div>
      <div className="flex-1 min-w-0">
        <h3 className="text-gray-400 text-xs font-semibold uppercase tracking-wider truncate">{title}</h3>
        <p className="text-2xl font-bold text-white mt-1 truncate">{value ?? '—'}</p>
        {sub && <p className="text-xs text-gray-500 mt-0.5 truncate">{sub}</p>}
      </div>
    </div>
  );
}

function TabBtn({ label, active, color = 'emerald', onClick }) {
  const colors = {
    emerald: 'text-emerald-400 border-emerald-400',
    purple: 'text-purple-400 border-purple-400',
    blue: 'text-blue-400 border-blue-400',
    amber: 'text-amber-400 border-amber-400',
    red: 'text-red-400 border-red-400',
  };
  return (
    <button
      onClick={onClick}
      className={`flex-1 py-4 text-xs lg:text-sm font-semibold text-center transition-colors ${
        active
          ? `${colors[color]} border-b-2 bg-gray-800/50`
          : 'text-gray-400 hover:text-gray-200'
      }`}
    >
      {label}
    </button>
  );
}

export default function Dashboard() {
  const [wsStatus, setWsStatus] = useState('Connecting...');
  const [liveRecords, setLiveRecords] = useState([]);
  const [activeTab, setActiveTab] = useState('manual');
  const [stats, setStats] = useState(null);

  // ── manual queue ──────────────────────────────────────────────────────
  const [inputText, setInputText] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);

  // ── spider ────────────────────────────────────────────────────────────
  const allStates = ["AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"];
  const [selectedStates, setSelectedStates] = useState([]);
  const [citySweep, setCitySweep] = useState(false);

  // ── orchestrator ────────────────────────────────────────────────────────
  const [autoEnabled, setAutoEnabled] = useState(false);
  const [autoQueueMin, setAutoQueueMin] = useState(500);
  const [autoStates, setAutoStates] = useState([]);
  const [autoAxes, setAutoAxes] = useState(['lastname']);
  const [isToggling, setIsToggling] = useState(false);
  const [isSeedingNow, setIsSeedingNow] = useState(false);
  const [orchStatus, setOrchStatus] = useState(null);  // live metrics from API
  const [orchLastMsg, setOrchLastMsg] = useState('');  // last seed message

  // ── database tab ──────────────────────────────────────────────────────
  const [dbRecords, setDbRecords] = useState([]);
  const [dbPage, setDbPage] = useState(1);
  const [dbTotalPages, setDbTotalPages] = useState(1);
  const [dbTotalCount, setDbTotalCount] = useState(0);
  const [isLoadingDb, setIsLoadingDb] = useState(false);

  // ── jobs tab ──────────────────────────────────────────────────────────
  const [jobs, setJobs] = useState([]);
  const [activeTasks, setActiveTasks] = useState([]);
  const [isLoadingJobs, setIsLoadingJobs] = useState(false);
  const [stateRuns, setStateRuns] = useState([]);
  const [isLoadingStateRuns, setIsLoadingStateRuns] = useState(false);
  const [stateRunActionId, setStateRunActionId] = useState(null);

  // ── accounts tab ──────────────────────────────────────────────────────
  const [credentials, setCredentials] = useState([]);
  const [proxies, setProxies] = useState([]);
  const [uuidBlob, setUuidBlob] = useState('');
  const [proxyBlob, setProxyBlob] = useState('');
  const [isLoadingAccounts, setIsLoadingAccounts] = useState(false);
  const [testResults, setTestResults] = useState({});
  const [accountsTab, setAccountsTab] = useState('uuids');
  const [useProxy, setUseProxy] = useState(true);

  // ── websocket ─────────────────────────────────────────────────────────
  useEffect(() => {
    const wsUrl = API.replace('http', 'ws') + '/ws/records/';
    const ws = new WebSocket(wsUrl);
    ws.onopen = () => setWsStatus('Connected (Live)');
    ws.onclose = () => setWsStatus('Disconnected');
    ws.onerror = () => setWsStatus('Connection Error');
    ws.onmessage = (event) => {
      const data = JSON.parse(event.data);
      if (data.type === 'new_record') setLiveRecords(prev => [data.data, ...prev]);
    };
    return () => ws.close();
  }, []);

  // ── settings state ────────────────────────────────────────────────────
  const [softLimit, setSoftLimit] = useState(80);

  // ── stats & settings ──────────────────────────────────────────────────
  const fetchSettings = useCallback(async () => {
    try {
      const r = await apiFetch('/api/settings/');
      if (r.ok) {
        const data = await r.json();
        const results = data.results || data;
        results.forEach(item => {
          if (item.key === 'auto_run_enabled') {
            setAutoEnabled(item.value === true || item.value === 'true');
          }
          if (item.key === 'auto_queue_min') setAutoQueueMin(parseInt(item.value, 10) || 500);
          if (item.key === 'auto_run_states') {
             try { setAutoStates(JSON.parse(item.value)); } 
             catch { setAutoStates(item.value ? item.value.split(',') : []); }
          }
          if (item.key === 'auto_run_axes') setAutoAxes(item.value ? item.value.split(',') : []);
          if (item.key === 'use_proxy') setUseProxy(item.value === 'true' || item.value === true);
          if (item.key === 'soft_limit') setSoftLimit(parseInt(item.value, 10) || 80);
        });
      }
    } catch {}
  }, []);

  const fetchOrchestratorStatus = useCallback(async () => {
    try {
      const r = await apiFetch('/api/settings/orchestrator_status/');
      if (r.ok) {
        const data = await r.json();
        setOrchStatus(data);
        setAutoEnabled(data.enabled);
        setAutoQueueMin(data.min_queue);
        // Only update local editable states from server if they haven't been modified yet
        // or during the first load. We'll rely on the server's truth for the status display.
        if (data.states !== undefined) {
           // We'll update the 'status' markers but keep autoStates as the user's workspace
        }
        if (data.axes?.length) setAutoAxes(data.axes);
      }
    } catch {}
  }, []);

  const fetchStateRuns = useCallback(async () => {
    setIsLoadingStateRuns(true);
    try {
      const r = await apiFetch('/api/state-runs/?ordering=-updated_at');
      if (r.ok) {
        const data = await r.json();
        setStateRuns(data.results || data);
      }
    } catch (e) {
      console.error(e);
    } finally {
      setIsLoadingStateRuns(false);
    }
  }, []);

  const fetchStats = useCallback(async () => {
    try {
      const r = await apiFetch('/api/stats/');
      if (r.ok) setStats(await r.json());
    } catch {}
  }, []);

  useEffect(() => {
    fetchSettings();
    // Initial fetch to populate local editable state
    apiFetch('/api/settings/orchestrator_status/')
      .then(r => r.json())
      .then(data => {
        setOrchStatus(data);
        setAutoStates(data.states || []);
        setAutoAxes(data.axes || ['lastname']);
      })
      .catch(() => {});
  }, [fetchSettings]);

  useEffect(() => {
    fetchStats();
    const id = setInterval(fetchStats, 5000);
    return () => clearInterval(id);
  }, [fetchStats]);

  // Poll orchestrator status when on that tab
  useEffect(() => {
    if (activeTab !== 'orchestrator') return;
    fetchOrchestratorStatus();
    fetchStateRuns();
    const id = setInterval(() => {
      fetchOrchestratorStatus();
      fetchStateRuns();
    }, 5000);
    return () => clearInterval(id);
  }, [activeTab, fetchOrchestratorStatus, fetchStateRuns]);

  // ── job & active task fetcher ─────────────────────────────────────────
  const fetchJobsData = useCallback(async () => {
    setIsLoadingJobs(true);
    try {
      const [jr, tr] = await Promise.all([
        apiFetch('/api/jobs/'),
        apiFetch('/api/tasks/?status=IN_PROGRESS'),
      ]);
      if (jr.ok) setJobs((await jr.json()).results || await jr.json());
      if (tr.ok) setActiveTasks((await tr.json()).results || await tr.json());
    } catch (e) { console.error(e); }
    finally { setIsLoadingJobs(false); }
  }, []);

  useEffect(() => {
    if (activeTab === 'jobs') {
      fetchJobsData();
      const id = setInterval(fetchJobsData, 5000);
      return () => clearInterval(id);
    }
  }, [activeTab, fetchJobsData]);

  // ── database fetcher ──────────────────────────────────────────────────
  const fetchDatabaseRecords = useCallback(async () => {
    setIsLoadingDb(true);
    try {
      const r = await apiFetch(`/api/records/?page=${dbPage}`);
      if (r.ok) {
        const data = await r.json();
        setDbRecords(data.results || data);
        if (data.count) {
          setDbTotalCount(data.count);
          setDbTotalPages(Math.ceil(data.count / 100));
        }
      }
    } catch (e) { console.error(e); }
    finally { setIsLoadingDb(false); }
  }, [dbPage]);

  useEffect(() => {
    if (activeTab === 'database') fetchDatabaseRecords();
  }, [activeTab, dbPage, fetchDatabaseRecords]);

  // ── accounts fetcher ──────────────────────────────────────────────────
  const fetchAccounts = useCallback(async () => {
    setIsLoadingAccounts(true);
    try {
      const [cr, pr] = await Promise.all([
        apiFetch('/api/credentials/'),
        apiFetch('/api/proxies/'),
      ]);
      if (cr.ok) {
        const data = await cr.json();
        setCredentials(data.results || data);
      }
      if (pr.ok) {
        const data = await pr.json();
        setProxies(data.results || data);
      }
    } catch (e) { console.error(e); }
    finally { setIsLoadingAccounts(false); }
  }, []);

  // ── (settings fetched globally above via fetchSettings) ───────────────

  useEffect(() => {
    if (activeTab === 'accounts') {
      fetchAccounts();
      fetchSettings();
      const id = setInterval(() => { fetchAccounts(); fetchSettings(); }, 15000);
      return () => clearInterval(id);
    }
  }, [activeTab, fetchAccounts, fetchSettings]);

  // ── handlers ──────────────────────────────────────────────────────────
  const handleQueueLookups = async () => {
    if (!inputText.trim()) return;
    setIsSubmitting(true);
    const targets = inputText.split('\n').map(l => l.trim()).filter(Boolean).map(line => {
      const parts = line.split(/\s+/);
      return {
        firstname: parts[0] || '',
        lastname: parts.length > 2 ? parts.slice(1, -1).join(' ') : (parts[1] || ''),
        state: parts.length > 2 ? parts[parts.length - 1] : (parts.length === 2 && parts[1].length === 2 ? parts[1] : ''),
      };
    });
    try {
      const r = await apiFetch('/api/jobs/create_batch/', {
        method: 'POST',
        body: JSON.stringify({ name: `Manual Job ${new Date().toLocaleTimeString()}`, targets }),
      });
      if (r.ok) setInputText('');
    } catch (e) { console.error(e); }
    finally { setIsSubmitting(false); }
  };

  const handleIgniteSpider = async () => {
    if (!selectedStates.length) return;
    setIsSubmitting(true);
    try {
      const r = await apiFetch('/api/jobs/ignite_spider/', {
        method: 'POST',
        body: JSON.stringify({
          name: `Spider Run (${selectedStates.length} States${citySweep ? ' + City' : ''})`,
          states: selectedStates,
          run_city_sweep: citySweep,
        }),
      });
      if (r.ok) setSelectedStates([]);
    } catch (e) { console.error(e); }
    finally { setIsSubmitting(false); }
  };

  const toggleState = (st) =>
    setSelectedStates(prev => prev.includes(st) ? prev.filter(s => s !== st) : [...prev, st]);

  const [stoppingJobs, setStoppingJobs] = useState([]); // Array of job IDs being stopped

  const handleStopJob = async (id) => {
    setStoppingJobs(prev => [...prev, id]);
    try {
      await apiFetch(`/api/jobs/${id}/stop/`, { method: 'POST' });
      await fetchJobsData();
    } catch (e) {
      console.error(e);
    } finally {
      setStoppingJobs(prev => prev.filter(jid => jid !== id));
    }
  };

  const handleResumeJob = async (id) => {
    await apiFetch(`/api/jobs/${id}/resume/`, { method: 'POST' });
    fetchJobsData();
  };

  const handleStopTask = async (id) => {
    await apiFetch(`/api/tasks/${id}/stop/`, { method: 'POST' });
    fetchJobsData();
  };

  const handleDeleteJob = async (id) => {
    if (window.confirm("Delete job and all associated tasks?")) {
      await apiFetch(`/api/jobs/${id}/`, { method: 'DELETE' });
      fetchJobsData();
    }
  };

  const handleExportCSV = (stateFilter = '') => {
    window.location.href = `${API}/api/export/${stateFilter ? `?state=${stateFilter}` : ''}`;
  };

  // ── accounts handlers ─────────────────────────────────────────────────
  const handleBulkAddUUIDs = async () => {
    if (!uuidBlob.trim()) return;
    const r = await apiFetch('/api/credentials/bulk_add/', {
      method: 'POST',
      body: JSON.stringify({ uuids: uuidBlob }),
    });
    if (r.ok) {
      setUuidBlob('');
      fetchAccounts();
    }
  };

  const handleTestCredential = async (id) => {
    setTestResults(p => ({ ...p, [id]: 'loading' }));
    const r = await apiFetch(`/api/credentials/${id}/test/`);
    const data = await r.json();
    setTestResults(p => ({ ...p, [id]: data.login_success }));
  };

  const handleResetCredential = async (id) => {
    await apiFetch(`/api/credentials/${id}/reset/`, { method: 'POST' });
    fetchAccounts();
  };

  const handleResetPool = async () => {
    if (window.confirm("Reactivate all UUIDs and reset their counters?")) {
      const r = await apiFetch('/api/credentials/reset_pool/', { method: 'POST' });
      if (r.ok) fetchAccounts();
    }
  };

  const handleUpdateSoftLimit = async (val) => {
    setSoftLimit(val);
    await apiFetch('/api/settings/set_value/', {
      method: 'POST',
      body: JSON.stringify({ key: 'soft_limit', value: val }),
    });
  };

  const handleDeleteCredential = async (id) => {
    if (window.confirm("Delete this credential?")) {
      await apiFetch(`/api/credentials/${id}/`, { method: 'DELETE' });
      setCredentials(c => c.filter(x => x.id !== id));
    }
  };

  const handleBulkAddProxies = async () => {
    if (!proxyBlob.trim()) return;
    const r = await apiFetch('/api/proxies/bulk_add/', {
      method: 'POST',
      body: JSON.stringify({ proxies: proxyBlob }),
    });
    if (r.ok) {
      setProxyBlob('');
      fetchAccounts();
    }
  };

  const handleToggleProxy = async (id) => {
    await apiFetch(`/api/proxies/${id}/toggle/`, { method: 'POST' });
    fetchAccounts();
  };

  const handleDeleteProxy = async (id) => {
    if (window.confirm("Delete this proxy?")) {
      await apiFetch(`/api/proxies/${id}/`, { method: 'DELETE' });
      setProxies(p => p.filter(x => x.id !== id));
    }
  };

  const handleToggleProxyGlobal = async () => {
    const r = await apiFetch('/api/settings/toggle/', {
      method: 'POST',
      body: JSON.stringify({ key: 'use_proxy' }),
    });
    if (r.ok) {
      const data = await r.json();
      setUseProxy(data.value);
    }
  };

  // ── orchestrator handlers ─────────────────────────────────────────────
  const handleToggleAutoRun = async () => {
    setIsToggling(true);
    try {
      const r = await apiFetch('/api/settings/toggle/', { method: 'POST', body: JSON.stringify({ key: 'auto_run_enabled' }) });
      if (r.ok) {
        const d = await r.json();
        const nowEnabled = d.value === true || d.value === 'true';
        setAutoEnabled(nowEnabled);
        // If just started, immediately trigger a seed so the user sees results fast
        if (nowEnabled) {
          await apiFetch('/api/settings/seed_now/', { method: 'POST' })
            .then(r2 => r2.json())
            .then(d2 => setOrchLastMsg(d2.message || 'Seeded.'))
            .catch(() => {});
        }
        setTimeout(fetchOrchestratorStatus, 500);
      }
    } finally {
      setIsToggling(false);
    }
  };

  const handleSeedNow = async () => {
    setIsSeedingNow(true);
    setOrchLastMsg('');
    try {
      const r = await apiFetch('/api/settings/seed_now/', { method: 'POST' });
      const d = await r.json();
      setOrchLastMsg(r.ok ? d.message : (d.error || 'Unknown error'));
      if (r.ok) setTimeout(fetchOrchestratorStatus, 1000);
    } catch (e) {
      setOrchLastMsg('Request failed: ' + e.message);
    } finally {
      setIsSeedingNow(false);
    }
  };

  const handleNewSession = async () => {
    if (!window.confirm('Start a new sweep session? This will clear all completed auto-run tasks so the orchestrator can re-seed a fresh A–Z sweep.')) return;
    setOrchLastMsg('');
    try {
      const r = await apiFetch('/api/settings/new_session/', { method: 'POST' });
      const d = await r.json();
      setOrchLastMsg(d.message || 'Session reset.');
      setTimeout(fetchOrchestratorStatus, 800);
    } catch (e) {
      setOrchLastMsg('Reset failed: ' + e.message);
    }
  };

  const handleSaveAutoSettings = async () => {
    setIsSubmitting(true);
    await apiFetch('/api/settings/set_value/', { method: 'POST', body: JSON.stringify({ key: 'auto_queue_min', value: autoQueueMin }) });
    // store states and axes as simple comma strings to safely fit in 255 chars
    await apiFetch('/api/settings/set_value/', { method: 'POST', body: JSON.stringify({ key: 'auto_run_states', value: autoStates.join(',') }) });
    await apiFetch('/api/settings/set_value/', { method: 'POST', body: JSON.stringify({ key: 'auto_run_axes', value: autoAxes.join(',') }) });
    setIsSubmitting(false);
  };

  const handleStateRunAction = async (id, action) => {
    setStateRunActionId(id);
    try {
      await apiFetch(`/api/state-runs/${id}/${action}/`, { method: 'POST' });
      await Promise.all([fetchStateRuns(), fetchOrchestratorStatus()]);
    } catch (e) {
      console.error(e);
    } finally {
      setStateRunActionId(null);
    }
  };

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <StatCard title="Total Records" value={stats?.total_records?.toLocaleString()} icon="📊" sub={`+${stats?.records_last_hour ?? 0}/hr`} />
        <StatCard title="Work Queue" value={stats ? stats.pending_tasks : null} icon="⚡" sub={`${stats?.in_progress_tasks ?? 0} active workers`} />
        <StatCard title="Active UUIDs" value={stats ? `${stats.active_credentials}/${stats.total_credentials}` : null} icon="🔑" />
        <StatCard title="Active Proxies" value={stats ? `${stats.active_proxies}/${stats.total_proxies}` : null} icon="🛡️" />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <div className="bg-gray-800 rounded-xl border border-gray-700 shadow-xl flex flex-col">
          <div className="flex border-b border-gray-700 overflow-x-auto no-scrollbar">
            <TabBtn label="Manual" active={activeTab === 'manual'} color="emerald" onClick={() => setActiveTab('manual')} />
            <TabBtn label="Spider" active={activeTab === 'spider'} color="purple" onClick={() => setActiveTab('spider')} />
            <TabBtn label="Auto Run" active={activeTab === 'orchestrator'} color="blue" onClick={() => setActiveTab('orchestrator')} />
            <TabBtn label="Jobs" active={activeTab === 'jobs'} color="red" onClick={() => setActiveTab('jobs')} />
            <TabBtn label="Database" active={activeTab === 'database'} color="emerald" onClick={() => setActiveTab('database')} />
            <TabBtn label="Pools" active={activeTab === 'accounts'} color="amber" onClick={() => setActiveTab('accounts')} />
          </div>

          <div className="p-6 flex-1 overflow-y-auto custom-scrollbar">
            {activeTab === 'manual' && (
              <div className="h-full flex flex-col">
                <p className="text-sm text-gray-400 mb-4 font-mono">Format: First Last State</p>
                <textarea value={inputText} onChange={e => setInputText(e.target.value)}
                  className="flex-1 w-full bg-gray-900 border border-gray-600 rounded-lg p-3 text-sm focus:outline-none focus:border-emerald-500 font-mono resize-none min-h-[250px]"
                  placeholder={"John Doe NY\nJane Smith CA"} />
                <button onClick={handleQueueLookups} disabled={isSubmitting || !inputText.trim()}
                  className="mt-4 w-full bg-emerald-600 hover:bg-emerald-500 disabled:opacity-50 text-white font-medium py-3 rounded-lg transition-colors">
                  {isSubmitting ? 'Queueing...' : 'Queue Lookups'}
                </button>
              </div>
            )}

            {activeTab === 'spider' && (
              <div className="h-full flex flex-col">
                <div className="flex justify-between items-end mb-4">
                  <p className="text-sm text-gray-400">Select states for crawler.</p>
                  <button onClick={() => setSelectedStates(selectedStates.length === allStates.length ? [] : allStates)}
                    className="text-xs text-purple-400 hover:text-purple-300 font-semibold">
                    {selectedStates.length === allStates.length ? 'Clear' : 'Select All'}
                  </button>
                </div>
                <div className="flex-1 bg-gray-900 border border-gray-600 rounded-lg p-3 overflow-y-auto custom-scrollbar max-h-[240px]">
                  <div className="grid grid-cols-5 gap-2">
                    {allStates.map(st => (
                      <button key={st} onClick={() => toggleState(st)}
                        className={`py-1 text-xs font-mono rounded border transition-colors ${selectedStates.includes(st) ? 'bg-purple-500/20 border-purple-500 text-purple-300' : 'bg-gray-800 border-gray-700 text-gray-400 hover:border-gray-500'}`}>{st}</button>
                    ))}
                  </div>
                </div>
                {/* City Sweep Toggle */}
                <div className="flex items-center justify-between mt-3 bg-gray-900/50 border border-gray-700 rounded-lg px-3 py-2">
                  <div>
                    <p className="text-xs font-bold text-gray-300">Include City Sweep</p>
                    <p className="text-[10px] text-gray-500">Run A–Z city axis sweep alongside name spider</p>
                  </div>
                  <button
                    onClick={() => setCitySweep(v => !v)}
                    className={`relative inline-flex h-5 w-9 items-center rounded-full transition-colors ${citySweep ? 'bg-purple-500' : 'bg-gray-700'}`}
                  >
                    <span className={`inline-block h-3 w-3 transform rounded-full bg-white transition-transform ${citySweep ? 'translate-x-5' : 'translate-x-1'}`} />
                  </button>
                </div>
                <button onClick={handleIgniteSpider} disabled={isSubmitting || !selectedStates.length}
                  className="mt-3 w-full bg-purple-600 hover:bg-purple-500 disabled:opacity-50 text-white font-bold py-3 rounded-lg transition-all shadow-lg shadow-purple-900/50">
                  {isSubmitting ? 'Igniting...' : `IGNITE SPIDER (${selectedStates.length}${citySweep ? ' + CITY' : ''})`}
                </button>
              </div>
            )}

            {activeTab === 'orchestrator' && (
              <div className="h-full flex flex-col gap-3">

                {/* LIVE STATUS BAR */}
                {/* LIVE STATUS HEADER */}
                <div className={`flex items-center gap-3 p-3 rounded-xl border transition-all ${
                  autoEnabled 
                    ? 'bg-emerald-500/10 border-emerald-500/40' 
                    : 'bg-gray-900 border-gray-700'
                }`}>
                  <div className={`w-2.5 h-2.5 rounded-full flex-shrink-0 ${autoEnabled ? 'bg-emerald-400 animate-pulse' : 'bg-gray-600'}`} />
                  <div className="flex-1 min-w-0">
                    <p className={`text-xs font-bold uppercase tracking-widest ${autoEnabled ? 'text-emerald-300' : 'text-gray-500'}`}>
                      {autoEnabled ? '🤖 Orchestrator Running' : '⏹ Orchestrator Stopped'}
                    </p>
                    {orchStatus && (
                      <p className="text-[10px] text-gray-500 font-mono mt-0.5">
                        {orchStatus.states?.length || 0} states · {orchStatus.axes?.length || 0} axes · {orchStatus.total_prime_slots || 0} prime slots
                      </p>
                    )}
                  </div>
                  <div className="flex gap-2">
                    {autoEnabled ? (
                      <button onClick={handleToggleAutoRun} disabled={isToggling}
                        className="flex items-center gap-1.5 bg-red-500/15 hover:bg-red-500 text-red-400 hover:text-white border border-red-500/40 px-3 py-1.5 rounded-lg text-xs font-bold transition-all active:scale-95 disabled:opacity-50">
                        {isToggling ? '⏳' : '⏹'} {isToggling ? 'Stopping...' : 'Stop'}
                      </button>
                    ) : (
                      <button onClick={handleToggleAutoRun} disabled={isToggling || !autoStates.length || !orchStatus?.credentials_ready}
                        title={!autoStates.length ? 'Select states first and save config' : ''}
                        className="flex items-center gap-1.5 bg-emerald-500/15 hover:bg-emerald-500 text-emerald-400 hover:text-white border border-emerald-500/40 px-3 py-1.5 rounded-lg text-xs font-bold transition-all active:scale-95 disabled:opacity-50">
                        {isToggling ? '⏳' : '▶'} {isToggling ? 'Starting...' : 'Start'}
                      </button>
                    )}
                    <button onClick={handleSeedNow} disabled={isSeedingNow || !autoStates.length || !orchStatus?.credentials_ready}
                      className="flex items-center gap-1.5 bg-blue-500/15 hover:bg-blue-600 text-blue-400 hover:text-white border border-blue-500/40 px-3 py-1.5 rounded-lg text-xs font-bold transition-all active:scale-95 disabled:opacity-50">
                      {isSeedingNow ? '⏳' : '⚡'} {isSeedingNow ? 'Seeding...' : 'Seed Now'}
                    </button>
                  </div>
                </div>


                {orchStatus && !orchStatus.credentials_ready && (
                  <div className="text-[11px] text-red-300 bg-red-500/10 border border-red-500/30 rounded p-2">
                    Add at least one active UUID in Pools before starting Auto Run.
                  </div>
                )}

                {/* LIVE METRICS GRID */}
                {orchStatus && (
                  <div className="grid grid-cols-4 gap-2">
                    {[
                      { label: 'Pending', value: orchStatus.pending, color: 'text-yellow-400' },
                      { label: 'Running', value: orchStatus.in_progress, color: 'text-blue-400' },
                      { label: 'Done', value: orchStatus.completed, color: 'text-emerald-400' },
                      { label: 'Failed', value: orchStatus.failed, color: 'text-red-400' },
                    ].map(m => (
                      <div key={m.label} className="bg-gray-900 border border-gray-800 rounded-lg p-2 text-center">
                        <p className={`text-sm font-bold font-mono ${m.color}`}>{m.value ?? '–'}</p>
                        <p className="text-[9px] text-gray-600 uppercase mt-0.5">{m.label}</p>
                      </div>
                    ))}
                  </div>
                )}

                {/* SWEEP PROGRESS BAR */}
                {orchStatus && orchStatus.total_prime_slots > 0 && (
                  <div className="space-y-1">
                    <div className="flex justify-between text-[10px] text-gray-500 font-mono">
                      <span>Sweep Progress</span>
                      <span>{orchStatus.completed || 0} / {orchStatus.total_prime_slots} prime slots done</span>
                    </div>
                    <div className="w-full bg-gray-800 rounded-full h-1.5 overflow-hidden">
                      <div
                        className="h-full rounded-full transition-all duration-700"
                        style={{
                          width: `${Math.min(((orchStatus.completed || 0) / orchStatus.total_prime_slots) * 100, 100)}%`,
                          background: 'linear-gradient(90deg, #10b981, #3b82f6)',
                        }}
                      />
                    </div>
                  </div>
                )}

                {/* LAST MESSAGE */}
                {orchLastMsg && (
                  <div className="text-[10px] font-mono text-gray-400 bg-gray-900/80 border border-gray-800 rounded p-2 leading-relaxed">
                    ✦ {orchLastMsg}
                  </div>
                )}

                {/* STATE RUN MONITOR */}
                <div className="bg-gray-900 border border-gray-700 rounded-lg p-3 space-y-2">
                  <div className="flex items-center justify-between">
                    <p className="text-[10px] font-bold text-gray-500 uppercase tracking-widest">State Runs</p>
                    <button onClick={fetchStateRuns} className="text-[10px] text-blue-400 hover:text-blue-300">⟳ Refresh</button>
                  </div>

                  {isLoadingStateRuns && <p className="text-xs text-gray-500">Loading state runs...</p>}
                  {!isLoadingStateRuns && stateRuns.length === 0 && (
                    <p className="text-xs text-gray-500">No state runs yet. Save config and seed to start tracking per-state progress.</p>
                  )}

                  <div className="space-y-2 max-h-48 overflow-y-auto custom-scrollbar pr-1">
                    {stateRuns.slice(0, 12).map(sr => {
                      const done = sr.total_primes > 0
                        ? Math.min((sr.primes_completed / sr.total_primes) * 100, 100)
                        : 0;
                      const actionBusy = stateRunActionId === sr.id;
                      return (
                        <div key={sr.id} className="border border-gray-700 rounded-lg p-2 bg-gray-800/60 space-y-1.5">
                          <div className="flex items-center justify-between gap-2">
                            <div className="flex items-center gap-2">
                              <span className="text-xs font-mono font-bold text-white">{sr.state}</span>
                              <StatusBadge status={sr.status} />
                            </div>
                            <div className="text-[10px] text-gray-500 font-mono">
                              {sr.tasks_completed}/{sr.total_tasks || 0} tasks
                            </div>
                          </div>
                          <div className="w-full h-1.5 rounded-full bg-gray-900 overflow-hidden">
                            <div className="h-full bg-blue-500 transition-all" style={{ width: `${done}%` }} />
                          </div>
                          <div className="flex items-center justify-between text-[10px] text-gray-500 font-mono">
                            <span>Primes {sr.primes_completed}/{sr.total_primes || 0}</span>
                            <span>{Math.round(done)}%</span>
                          </div>
                          <div className="flex gap-1">
                            <button disabled={actionBusy || sr.status !== 'RUNNING'} onClick={() => handleStateRunAction(sr.id, 'pause')}
                              className="px-2 py-1 text-[10px] rounded border border-amber-500/30 text-amber-300 disabled:opacity-40">Pause</button>
                            <button disabled={actionBusy || sr.status !== 'PAUSED'} onClick={() => handleStateRunAction(sr.id, 'resume')}
                              className="px-2 py-1 text-[10px] rounded border border-blue-500/30 text-blue-300 disabled:opacity-40">Resume</button>
                            <button disabled={actionBusy || ['COMPLETED', 'FAILED'].includes(sr.status)} onClick={() => handleStateRunAction(sr.id, 'stop')}
                              className="px-2 py-1 text-[10px] rounded border border-red-500/30 text-red-300 disabled:opacity-40">Stop</button>
                            <button disabled={actionBusy} onClick={() => handleStateRunAction(sr.id, 'refresh_metrics')}
                              className="ml-auto px-2 py-1 text-[10px] rounded border border-gray-600 text-gray-300 disabled:opacity-40">Metrics</button>
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </div>

                {/* CONFIGURATION */}
                <div className="bg-gray-900 border border-gray-700 rounded-lg p-3 space-y-3 overflow-y-auto custom-scrollbar flex-1">
                  <p className="text-[10px] font-bold text-gray-500 uppercase tracking-widest">Configuration</p>

                  <div>
                    <label className="text-xs text-gray-400 font-bold mb-1 block">Queue Min Target</label>
                    <input type="number" value={autoQueueMin} onChange={e => setAutoQueueMin(e.target.value)}
                      className="w-full bg-gray-800 border border-gray-700 rounded p-2 text-sm text-gray-200 focus:outline-none focus:border-blue-500" />
                    <p className="text-[10px] text-gray-500 mt-1">Seed until pending queue hits this count.</p>
                  </div>

                  <div>
                    <div className="flex justify-between items-center mb-1.5">
                       <label className="text-xs text-gray-400 font-bold">Target States</label>
                       <div className="flex gap-2">
                         <button onClick={() => setAutoStates(allStates)}
                           className="text-[10px] text-blue-400 hover:text-blue-300">
                           Select All
                         </button>
                         <span className="text-gray-600 text-[10px]">|</span>
                         <button onClick={() => setAutoStates([])}
                           className="text-[10px] text-red-400 hover:text-red-300">
                           Clear All
                         </button>
                       </div>
                    </div>
                    <div className="grid grid-cols-6 gap-1 max-h-[100px] overflow-y-auto custom-scrollbar pr-1">
                      {allStates.map(st => {
                        const isSelected = autoStates.includes(st);
                        const isLive = orchStatus?.states?.includes(st);
                        return (
                          <button key={st} onClick={() => setAutoStates(prev => prev.includes(st) ? prev.filter(x => x !== st) : [...prev, st])}
                            className={`py-1 text-[10px] font-mono rounded border transition-colors ${
                              isSelected 
                                ? 'bg-blue-500/20 border-blue-500 text-blue-300' 
                                : 'bg-gray-800 border-gray-700 text-gray-500 hover:border-gray-600'
                            } ${isLive ? 'ring-1 ring-emerald-500/50' : ''}`}>
                            {st}
                          </button>
                        );
                      })}
                    </div>
                  </div>

                  <div>
                    <label className="text-xs text-gray-400 font-bold mb-1.5 block">Sweep Axes</label>
                    <div className="flex gap-2 text-xs">
                      {['lastname', 'firstname', 'city'].map(ax => (
                        <button key={ax} onClick={() => setAutoAxes(prev => prev.includes(ax) ? prev.filter(x => x !== ax) : [...prev, ax])}
                          className={`px-3 py-1.5 rounded border capitalize transition-all ${
                            autoAxes.includes(ax) ? 'bg-blue-500 text-white border-blue-400 shadow shadow-blue-900/50' 
                            : 'bg-gray-800 border-gray-700 text-gray-400 hover:border-gray-600'}`}>
                          {ax}
                        </button>
                      ))}
                    </div>
                  </div>

                  <div className="flex gap-2 pt-1">
                    <button onClick={handleSaveAutoSettings} disabled={isSubmitting}
                      className="flex-1 bg-blue-600 hover:bg-blue-500 active:scale-[0.98] text-white font-bold py-2.5 rounded-lg transition-all shadow-lg shadow-blue-900/50">
                      {isSubmitting ? 'Saving...' : '💾 Save'}
                    </button>
                    <button onClick={handleNewSession}
                      className="px-4 bg-gray-800 hover:bg-red-500/20 text-gray-500 hover:text-red-400 border border-gray-700 hover:border-red-500/40 font-bold py-2.5 rounded-lg transition-all text-xs"
                      title="Clear completed tasks and start a fresh A–Z sweep">
                      🔄 New Session
                    </button>
                  </div>
                </div>
              </div>
            )}


            {activeTab === 'jobs' && (
              <div className="flex flex-col gap-4">
                <p className="text-xs text-gray-500 uppercase tracking-widest font-bold">Recent Jobs</p>
                <div className="space-y-3">
                  {jobs.length === 0 && <p className="text-sm text-gray-500 italic">No jobs yet. Start a manual/spider/auto run to see activity.</p>}
                  {jobs.slice(0, 5).map(j => (
                    <div key={j.id} className="p-3 rounded-lg bg-gray-900 border border-gray-700 space-y-2">
                       <div className="flex justify-between items-start">
                         <div className="flex-1 min-w-0 flex items-center gap-2 pr-2">
                            <span className="text-sm font-bold text-gray-200 truncate">{j.name}</span>
                            <StatusBadge status={j.status} />
                         </div>
                         {j.status === 'RUNNING' && (
                            <button onClick={() => handleStopJob(j.id)} disabled={stoppingJobs.includes(j.id)}
                              className={`text-[10px] px-2 py-0.5 rounded border transition-colors ${stoppingJobs.includes(j.id) ? 'bg-gray-700 text-gray-400 border-gray-600' : 'bg-red-900/40 text-red-400 border-red-500/30 hover:bg-red-900/60'}`}>
                              {stoppingJobs.includes(j.id) ? 'Stopping...' : 'KILL ALL'}
                            </button>
                         )}
                         {j.status === 'STOPPED' && (
                            <button onClick={() => { if(window.confirm("Resume queued tasks?")) handleResumeJob(j.id); }}
                              className="text-[10px] bg-blue-500/10 text-blue-400 hover:bg-blue-500 hover:text-white px-2 py-0.5 border border-blue-500/30 rounded transition-colors">
                              RESUME
                            </button>
                         )}
                         <button onClick={() => handleDeleteJob(j.id)} 
                            className="text-[10px] bg-gray-800 text-gray-400 hover:text-red-400 px-2 py-0.5 border border-gray-700/50 hover:border-red-500/30 rounded transition-colors ml-2" 
                            title="Delete Job and Tasks">
                            ✕
                         </button>
                      </div>
                      <div className="w-full bg-gray-800 h-1 rounded-full overflow-hidden">
                        <div className="bg-emerald-500 h-full transition-all duration-500" style={{ width: `${(j.completed_tasks / j.tasks_count) * 100}%` }} />
                      </div>
                      <div className="flex justify-between text-[10px] text-gray-500 font-mono">
                        <span>{j.completed_tasks} / {j.tasks_count} DONE</span>
                        <span>{Math.round((j.completed_tasks / j.tasks_count) * 100) || 0}%</span>
                      </div>
                    </div>
                  ))}
                </div>
                <div className="flex justify-between items-center mt-2">
                   <button onClick={fetchJobsData} className="text-xs text-blue-400 hover:text-blue-300 transition-colors">⟳ Refresh Jobs</button>
                   <button onClick={fetchJobsData} className="text-xs text-gray-500 hover:text-white transition-colors">See all job history →</button>
                </div>
              </div>
            )}

            {activeTab === 'database' && (
              <div className="h-full flex flex-col items-center justify-center text-center gap-6">
                <div className="text-5xl">🗄️</div>
                <div className="space-y-2">
                  <h3 className="text-lg font-bold text-gray-200">Historical Archive</h3>
                  <p className="text-sm text-gray-400 max-w-xs">{dbTotalCount.toLocaleString()} PII records collected to date.</p>
                </div>
                <div className="flex flex-col gap-2 w-full max-w-[200px]">
                  <button onClick={fetchDatabaseRecords} className="w-full bg-blue-600 hover:bg-blue-500 text-white font-medium py-2 rounded-lg flex items-center justify-center gap-2">⟳ Refresh</button>
                  <button onClick={() => handleExportCSV()} className="w-full bg-gray-700 hover:bg-gray-600 text-white font-medium py-2 rounded-lg">⬇ Export All</button>
                </div>
              </div>
            )}

            {activeTab === 'accounts' && (
              <div className="flex flex-col gap-4">
                <div className="flex items-center justify-between bg-gray-900/50 p-3 rounded-lg border border-gray-700 mb-2">
                  <div className="flex items-center gap-3">
                    <span className="text-xl">🛡️</span>
                    <div>
                      <p className="text-xs font-bold text-gray-200">ROUTING MODE</p>
                      <p className="text-[10px] text-gray-500 uppercase">{useProxy ? 'Proxied Traffic (Stealth)' : 'Direct Connection (Fast)'}</p>
                    </div>
                  </div>
                  <button 
                    onClick={handleToggleProxyGlobal}
                    className={`relative inline-flex h-6 w-11 items-center rounded-full transition-colors focus:outline-none ${useProxy ? 'bg-amber-500' : 'bg-gray-700'}`}
                  >
                    <span className={`inline-block h-4 w-4 transform rounded-full bg-white transition-transform ${useProxy ? 'translate-x-6' : 'translate-x-1'}`} />
                  </button>
                </div>

                <div className="grid grid-cols-2 gap-2 mb-2">
                  <div className="bg-gray-900/50 p-3 rounded-lg border border-gray-700 flex flex-col gap-1">
                    <p className="text-[10px] font-bold text-gray-500 uppercase">Soft Limit</p>
                    <div className="flex items-center gap-2">
                      <input 
                        type="number" 
                        value={softLimit} 
                        onChange={(e) => handleUpdateSoftLimit(parseInt(e.target.value) || 0)}
                        className="bg-gray-800 border border-gray-700 rounded px-2 py-1 text-xs w-20 text-amber-500 font-mono focus:outline-none focus:border-amber-500"
                      />
                      <span className="text-[10px] text-gray-600">reqs/key</span>
                    </div>
                  </div>
                  <button 
                    onClick={handleResetPool}
                    className="bg-emerald-900/20 border border-emerald-500/30 hover:bg-emerald-900/40 text-emerald-400 rounded-lg p-2 flex flex-col items-center justify-center gap-1 transition-all"
                  >
                    <span className="text-xs font-black">RESET POOL</span>
                    <span className="text-[9px] uppercase opacity-60">Reactivate all</span>
                  </button>
                </div>

                <div className="flex gap-1 bg-gray-900 p-1 rounded-lg">
                  <button onClick={() => setAccountsTab('uuids')} className={`flex-1 py-1.5 text-xs font-bold rounded-md transition-all ${accountsTab === 'uuids' ? 'bg-amber-500 text-black' : 'text-gray-400 hover:text-gray-200'}`}>UUIDS</button>
                  <button onClick={() => setAccountsTab('proxies')} className={`flex-1 py-1.5 text-xs font-bold rounded-md transition-all ${accountsTab === 'proxies' ? 'bg-amber-500 text-black' : 'text-gray-400 hover:text-gray-200'}`}>PROXIES</button>
                </div>
                {accountsTab === 'uuids' ? (
                  <textarea value={uuidBlob} onChange={e => setUuidBlob(e.target.value)} rows={5} className="w-full bg-gray-900 border border-gray-700 rounded-lg p-3 text-xs font-mono focus:border-amber-500 outline-none" placeholder="Paste UUIDs here..." />
                ) : (
                  <textarea value={proxyBlob} onChange={e => setProxyBlob(e.target.value)} rows={5} className="w-full bg-gray-900 border border-gray-700 rounded-lg p-3 text-xs font-mono focus:border-amber-500 outline-none" placeholder="Paste proxies here..." />
                )}
                <button onClick={accountsTab === 'uuids' ? handleBulkAddUUIDs : handleBulkAddProxies} className="w-full bg-amber-600 hover:bg-amber-500 text-black text-sm font-black py-2.5 rounded-lg transition-all shadow-lg shadow-amber-900/30">
                  {accountsTab === 'uuids' ? '+ ADD TO UUID POOL' : '+ ADD TO PROXY POOL'}
                </button>
              </div>
            )}
          </div>
        </div>

        <div className="lg:col-span-2 bg-gray-800 rounded-xl border border-gray-700 shadow-xl flex flex-col" style={{ height: '650px' }}>
          {activeTab === 'jobs' ? (
            <>
              <div className="px-6 py-4 border-b border-gray-700 flex justify-between items-center sticky top-0 bg-gray-800/90 backdrop-blur z-10">
                <h2 className="font-bold text-lg text-red-400 flex items-center gap-2">Running Tasks ⚡</h2>
                <div className="flex gap-2">
                   <button onClick={fetchJobsData} className="text-xs bg-gray-700 px-3 py-1 rounded text-gray-300 hover:bg-gray-600">Refresh</button>
                </div>
              </div>
              <div className="overflow-y-auto flex-1 custom-scrollbar">
                {activeTasks.length === 0 ? (
                  <div className="flex flex-col items-center justify-center h-full text-gray-600 gap-2">
                    <div className="text-4xl opacity-20">💤</div>
                    <p className="text-sm">No tasks currently executing on workers.</p>
                  </div>
                ) : (
                  <table className="w-full text-left text-sm">
                    <thead className="text-[10px] text-gray-500 uppercase tracking-widest bg-gray-900/50 sticky top-0 backdrop-blur">
                      <tr>
                        <th className="px-6 py-3">Task ID / Target</th>
                        <th className="px-6 py-3">Axis</th>
                        <th className="px-6 py-3">Status</th>
                        <th className="px-6 py-3">Job Ref</th>
                        <th className="px-6 py-3 text-right">Actions</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-700/30">
                      {activeTasks.map(t => (
                        <tr key={t.id} className="hover:bg-emerald-500/5 transition-colors group">
                          <td className="px-6 py-4">
                            <div className="text-gray-300 font-bold">
                              {t.axis === 'city'
                                ? `[CITY] ${t.city} (${t.state})`
                                : `${t.firstname} ${t.lastname} ${t.state}`}
                            </div>
                            <div className="text-[10px] text-gray-500 font-mono">{t.celery_task_id?.slice(0, 12) || 'NO_ID'}...</div>
                          </td>
                          <td className="px-6 py-4">
                            {{
                              'lastname': <span className="text-[10px] px-1.5 py-0.5 rounded bg-emerald-900/30 text-emerald-400 border border-emerald-500/20 font-mono">LAST</span>,
                              'firstname': <span className="text-[10px] px-1.5 py-0.5 rounded bg-blue-900/30 text-blue-400 border border-blue-500/20 font-mono">FIRST</span>,
                              'city': <span className="text-[10px] px-1.5 py-0.5 rounded bg-purple-900/30 text-purple-400 border border-purple-500/20 font-mono">CITY</span>,
                              'zip': <span className="text-[10px] px-1.5 py-0.5 rounded bg-amber-900/30 text-amber-400 border border-amber-500/20 font-mono">ZIP</span>,
                            }[t.axis] || <span className="text-gray-500 text-[10px]">{t.axis}</span>}
                          </td>
                          <td className="px-6 py-4"><StatusBadge status={t.status} /></td>
                          <td className="px-6 py-4 text-xs text-gray-500 truncate max-w-[100px]">{jobs.find(j => j.id === t.job)?.name || t.job}</td>
                          <td className="px-6 py-4 text-right">
                             <button onClick={() => handleStopTask(t.id)} className="opacity-0 group-hover:opacity-100 bg-red-900/40 text-red-400 text-[10px] px-2 py-1 rounded border border-red-500/20 hover:bg-red-900/60 transition-all">STOP</button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}
              </div>
            </>
          ) : activeTab === 'database' ? (
            <>
              <div className="px-6 py-4 border-b border-gray-700 flex justify-between items-center sticky top-0 bg-gray-800/90 backdrop-blur z-10">
                <h2 className="font-bold text-lg text-blue-400">Archive Explorer</h2>
                <div className="flex items-center gap-3">
                  <span className="text-[10px] text-gray-500 font-mono">PAGE {dbPage} / {dbTotalPages || 1}</span>
                  <div className="flex border border-gray-700 rounded overflow-hidden">
                    <button onClick={() => setDbPage(p => Math.max(1, p - 1))} disabled={dbPage === 1} className="px-3 py-1 bg-gray-900 text-gray-400 hover:bg-gray-700 disabled:opacity-20 border-r border-gray-700">‹</button>
                    <button onClick={() => setDbPage(p => Math.min(dbTotalPages, p + 1))} disabled={dbPage >= dbTotalPages} className="px-3 py-1 bg-gray-900 text-gray-400 hover:bg-gray-700 disabled:opacity-20">›</button>
                  </div>
                </div>
              </div>
              <div className="overflow-y-auto flex-1 custom-scrollbar">
                {isLoadingDb ? (
                   <div className="flex items-center justify-center h-full text-blue-400/50 animate-pulse">Scanning archive...</div>
                ) : (
                  <table className="w-full text-left text-sm text-gray-400">
                    <thead className="text-[10px] text-gray-500 uppercase bg-gray-900/50 sticky top-0 backdrop-blur">
                      <tr><th className="px-6 py-3">Name</th><th className="px-6 py-3">SSN</th><th className="px-6 py-3">DOB</th><th className="px-6 py-3">Location</th></tr>
                    </thead>
                    <tbody className="divide-y divide-gray-700/30">
                      {dbRecords.map((r, i) => (
                        <tr key={r.id || i} className="hover:bg-blue-500/5 transition-colors">
                          <td className="px-6 py-3 font-bold text-gray-300">{r.firstname} {r.lastname}</td>
                          <td className="px-6 py-3 font-mono text-xs text-blue-300/80">{r.ssn || '—'}</td>
                          <td className="px-6 py-3 text-xs">{r.dob || '—'}</td>
                          <td className="px-6 py-3 text-[10px] uppercase">{r.address}, {r.city}, {r.state} {r.zip_code}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}
              </div>
            </>
          ) : activeTab === 'accounts' ? (
            <>
              <div className="px-6 py-4 border-b border-gray-700 flex justify-between items-center bg-gray-800/90 backdrop-blur sticky top-0 z-10">
                <h2 className="font-bold text-lg text-amber-500">{accountsTab === 'uuids' ? 'UUID Infrastructure' : 'Proxy Network'}</h2>
                <button onClick={fetchAccounts} className="text-[10px] font-bold text-gray-500 hover:text-white uppercase">Force Sync</button>
              </div>
              <div className="overflow-y-auto flex-1 custom-scrollbar">
                {accountsTab === 'uuids' ? (
                  <table className="w-full text-left text-sm">
                    <thead className="text-[10px] text-gray-500 uppercase bg-gray-900/50 sticky top-0"><tr><th className="px-6 py-3">Credential ID</th><th className="px-6 py-3">Status</th><th className="px-6 py-3">Usage</th><th className="px-6 py-3 text-right">Actions</th></tr></thead>
                    <tbody className="divide-y divide-gray-700/30">
                      {credentials.map(c => (
                        <tr key={c.id} className="hover:bg-amber-500/5 group text-gray-400">
                          <td className="px-6 py-4 font-mono text-[11px] font-bold">{c.uuid.slice(0, 24)}…</td>
                          <td className="px-6 py-4"><StatusBadge status={c.is_active ? 'Active' : 'Exhausted'} /></td>
                          <td className="px-6 py-4 text-xs font-mono">{c.request_count} reqs</td>
                          <td className="px-6 py-4 text-right">
                            <div className="flex gap-1 justify-end opacity-0 group-hover:opacity-100 transition-opacity">
                              <button onClick={() => handleTestCredential(c.id)} className="p-1 px-2 rounded bg-gray-700 text-[10px] text-white">TEST</button>
                              <button onClick={() => handleResetCredential(c.id)} className="p-1 px-2 rounded bg-emerald-900 text-[10px] text-emerald-300">RESET</button>
                              <button onClick={() => handleDeleteCredential(c.id)} className="p-1 px-2 rounded bg-red-900 text-white text-[10px]">✕</button>
                            </div>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                ) : (
                  <table className="w-full text-left text-sm">
                    <thead className="text-[10px] text-gray-500 uppercase bg-gray-900/50 sticky top-0"><tr><th className="px-6 py-3">Endpoint</th><th className="px-6 py-3">Health</th><th className="px-6 py-3">Failures</th><th className="px-6 py-3 text-right">Actions</th></tr></thead>
                    <tbody className="divide-y divide-gray-700/30">
                      {proxies.map(p => (
                        <tr key={p.id} className="hover:bg-amber-500/5 group text-gray-400">
                          <td className="px-6 py-4 font-mono text-[11px] truncate max-w-[200px]">{p.masked_url}</td>
                          <td className="px-6 py-4"><StatusBadge status={p.is_active ? 'On' : 'Off'} /></td>
                          <td className="px-6 py-4 text-xs font-mono">{p.fail_count} err</td>
                          <td className="px-6 py-4 text-right">
                            <div className="flex gap-1 justify-end opacity-0 group-hover:opacity-100 transition-opacity">
                              <button onClick={() => handleToggleProxy(p.id)} className="p-1 px-2 rounded bg-gray-700 text-[10px] text-white uppercase">{p.is_active ? 'Kill' : 'Ignite'}</button>
                              <button onClick={() => handleDeleteProxy(p.id)} className="p-1 px-2 rounded bg-red-900 text-white text-[10px]">✕</button>
                            </div>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}
              </div>
            </>
          ) : (
            <>
              <div className="px-6 py-4 border-b border-gray-700 flex justify-between items-center bg-gray-800/90 backdrop-blur sticky top-0 z-10">
                <h2 className="font-bold text-lg flex items-center gap-2">Live Stream {wsStatus.includes('Live') && <span className="relative flex h-2 w-2"><span className="animate-ping absolute h-full w-full rounded-full bg-emerald-400 opacity-75"/><span className="h-full w-full rounded-full bg-emerald-500"/></span>}</h2>
                <div className="flex gap-2">
                  <span className="text-[10px] text-emerald-400 font-bold uppercase py-1 px-2 bg-emerald-500/10 rounded border border-emerald-500/20">{wsStatus}</span>
                  <button onClick={() => setLiveRecords([])} className="text-[10px] text-gray-500 hover:text-white uppercase transition-colors">Clear Local</button>
                </div>
              </div>
              <div className="overflow-y-auto flex-1 custom-scrollbar">
                {liveRecords.length === 0 ? (
                  <div className="flex flex-col items-center justify-center h-full text-gray-600 gap-2">
                    <div className="text-4xl opacity-20 animate-pulse">📡</div>
                    <p className="text-sm">Listening for incoming data packets...</p>
                  </div>
                ) : (
                  <table className="w-full text-left text-sm">
                    <thead className="text-[10px] text-gray-500 uppercase bg-gray-900/50 sticky top-0"><tr><th className="px-6 py-3">Name</th><th className="px-6 py-3">SSN</th><th className="px-6 py-3">DOB</th><th className="px-6 py-3">Location</th></tr></thead>
                    <tbody className="divide-y divide-gray-700/20">
                      {liveRecords.map((r, i) => (
                        <tr key={i} className="hover:bg-emerald-500/5 animate-fade-in group text-gray-300">
                          <td className="px-6 py-4 font-bold text-emerald-400/90">{r.firstname} {r.lastname}</td>
                          <td className="px-6 py-4 font-mono text-gray-400">{r.ssn || '—'}</td>
                          <td className="px-6 py-4 text-xs">{r.dob || '—'}</td>
                          <td className="px-6 py-4 text-[10px] text-gray-500 uppercase">{r.address}, {r.city}, {r.state} {r.zip_code}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  );
}
