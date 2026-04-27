import { Link, useLocation } from 'react-router-dom';
import { AlertTriangle } from 'lucide-react';

export function NotFound() {
  const location = useLocation();

  return (
    <div className="min-h-full flex items-center justify-center p-8">
      <div className="bg-white rounded-2xl shadow-sm border border-gray-100 p-8 max-w-md w-full text-center">
        <div className="w-16 h-16 bg-amber-50 rounded-2xl flex items-center justify-center mx-auto mb-4">
          <AlertTriangle size={28} className="text-amber-500" />
        </div>
        <h1 className="text-2xl font-bold text-gray-900 mb-2">404 — nie znaleziono</h1>
        <p className="text-sm text-gray-500 mb-1">
          Ścieżka{' '}
          <code className="px-1.5 py-0.5 bg-gray-100 rounded text-gray-700 text-xs">
            {location.pathname}
          </code>{' '}
          nie istnieje.
        </p>
        <p className="text-xs text-gray-400 mb-6">
          Sprawdź adres lub wróć do kolejki przeglądu.
        </p>
        <Link
          to="/queue"
          className="inline-block bg-blue-600 hover:bg-blue-700 text-white font-semibold py-2.5 px-6 rounded-xl transition-colors"
        >
          Wróć do Review Queue
        </Link>
      </div>
    </div>
  );
}
