import { useState } from 'react';
import Dashboard from './components/Dashboard';
import AutoRunManager from './components/AutoRunManager';

function App() {
  const [page, setPage] = useState('overview');

  return (
    <div className="min-h-screen bg-gray-900 text-gray-100 font-sans">
      <nav className="bg-gray-800 border-b border-gray-700 px-6 py-4 flex items-center justify-between">
        <div className="flex items-center space-x-3">
          <div className="w-8 h-8 rounded bg-emerald-500 flex items-center justify-center text-gray-900 font-bold text-xl">H</div>
          <span className="text-xl tracking-tight font-semibold">Hushra Datahub</span>
        </div>
        <div className="flex space-x-2 bg-gray-900/70 border border-gray-700 rounded-lg p-1">
          <button
            onClick={() => setPage('overview')}
            className={`px-3 py-1.5 rounded-md text-sm transition-colors ${page === 'overview' ? 'bg-emerald-500 text-gray-900 font-bold' : 'text-gray-300 hover:text-white'}`}
          >
            Overview
          </button>
          <button
            onClick={() => setPage('auto-run')}
            className={`px-3 py-1.5 rounded-md text-sm transition-colors ${page === 'auto-run' ? 'bg-blue-500 text-white font-bold' : 'text-gray-300 hover:text-white'}`}
          >
            Auto Run Manager
          </button>
        </div>
      </nav>

      <main className="max-w-7xl mx-auto p-6 space-y-6">
        {page === 'overview' ? <Dashboard /> : <AutoRunManager />}
      </main>
    </div>
  );
}

export default App;
