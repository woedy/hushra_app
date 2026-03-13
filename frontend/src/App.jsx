import { useState } from 'react';
import Dashboard from './components/Dashboard';

function App() {
  return (
    <div className="min-h-screen bg-gray-900 text-gray-100 font-sans">
      <nav className="bg-gray-800 border-b border-gray-700 px-6 py-4 flex items-center justify-between">
        <div className="flex items-center space-x-3">
          <div className="w-8 h-8 rounded bg-emerald-500 flex items-center justify-center text-gray-900 font-bold text-xl">H</div>
          <span className="text-xl tracking-tight font-semibold">Hushra Datahub</span>
        </div>
        <div className="flex space-x-4">
          <button className="text-gray-400 hover:text-white transition-colors">Settings</button>
          <button className="text-gray-400 hover:text-white transition-colors">Documentation</button>
        </div>
      </nav>
      
      <main className="max-w-7xl mx-auto p-6 space-y-6">
        <Dashboard />
      </main>
    </div>
  );
}

export default App;
