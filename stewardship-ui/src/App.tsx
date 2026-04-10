import { BrowserRouter, Routes, Route, NavLink, Navigate } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { MsalProvider, AuthenticatedTemplate, UnauthenticatedTemplate } from '@azure/msal-react';
import { msalInstance, MOCK_MODE } from './api/mdmApi';
import { ReviewQueue } from './components/ReviewQueue/ReviewQueue';
import { PairDetail } from './components/PairDetail/PairDetail';
import { GoldenViewer } from './components/GoldenViewer/GoldenViewer';
import { LayoutDashboard, ListChecks, MapPin, Settings } from 'lucide-react';
import { cn } from './lib/utils';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: { staleTime: 30_000, retry: 1 },
  },
});

function NavItem({ to, label, icon }: { to: string; label: string; icon: React.ReactNode }) {
  return (
    <NavLink to={to} className={({ isActive }) =>
      cn("flex items-center gap-2 px-3 py-2 rounded-lg text-sm transition-colors",
         isActive ? "bg-blue-50 text-blue-700 font-semibold"
                  : "text-gray-600 hover:bg-gray-100")
    }>
      {icon}
      {label}
    </NavLink>
  );
}

function Layout({ children }: { children: React.ReactNode }) {
  return (
    <div className="flex h-screen bg-gray-50 overflow-hidden">
      {/* Sidebar */}
      <aside className="w-56 bg-white border-r border-gray-100 flex flex-col p-4 gap-1 shrink-0">
        <div className="mb-4 px-2">
          <div className="flex items-center gap-2">
            <div className="w-7 h-7 bg-blue-600 rounded-lg flex items-center justify-center">
              <MapPin size={14} className="text-white" />
            </div>
            <div>
              <p className="text-sm font-bold text-gray-900">MDM</p>
              <p className="text-xs text-gray-400">L'Osteria</p>
            </div>
          </div>
        </div>
        <NavItem to="/queue"  label="Review Queue" icon={<ListChecks size={16} />} />
        <NavItem to="/golden" label="Golden Records" icon={<MapPin size={16} />} />
        <NavItem to="/config" label="Konfiguracja" icon={<Settings size={16} />} />
      </aside>

      {/* Main content */}
      <main className="flex-1 overflow-y-auto">{children}</main>
    </div>
  );
}

function LoginPage() {
  return (
    <div className="min-h-screen bg-gray-50 flex items-center justify-center">
      <div className="bg-white rounded-2xl shadow-lg border border-gray-100 p-8 max-w-sm w-full text-center">
        <div className="w-16 h-16 bg-blue-600 rounded-2xl flex items-center justify-center mx-auto mb-4">
          <MapPin size={28} className="text-white" />
        </div>
        <h1 className="text-2xl font-bold text-gray-900 mb-1">MDM Stewardship</h1>
        <p className="text-gray-400 text-sm mb-6">L'Osteria Business Location</p>
        <button
          onClick={() => msalInstance.loginPopup({ scopes: ['User.Read'] })}
          className="w-full bg-blue-600 hover:bg-blue-700 text-white font-semibold py-2.5 px-4 rounded-xl transition-colors"
        >
          Zaloguj przez Azure AD
        </button>
      </div>
    </div>
  );
}

export default function App() {
  // Mock mode: pomiń Azure AD — idealne do local dev bez konfiguracji
  if (MOCK_MODE) {
    return (
      <QueryClientProvider client={queryClient}>
        <BrowserRouter>
          <Layout>
            <Routes>
              <Route path="/" element={<Navigate to="/queue" replace />} />
              <Route path="/queue"            element={<ReviewQueue />} />
              <Route path="/pairs/:pairId"    element={<PairDetail />} />
              <Route path="/golden/:locationHk" element={<GoldenViewer />} />
              <Route path="*" element={<Navigate to="/queue" replace />} />
            </Routes>
          </Layout>
        </BrowserRouter>
      </QueryClientProvider>
    );
  }

  return (
    <MsalProvider instance={msalInstance}>
      <QueryClientProvider client={queryClient}>
        <UnauthenticatedTemplate>
          <LoginPage />
        </UnauthenticatedTemplate>
        <AuthenticatedTemplate>
          <BrowserRouter>
            <Layout>
              <Routes>
                <Route path="/" element={<Navigate to="/queue" replace />} />
                <Route path="/queue"            element={<ReviewQueue />} />
                <Route path="/pairs/:pairId"    element={<PairDetail />} />
                <Route path="/golden/:locationHk" element={<GoldenViewer />} />
                <Route path="*" element={<Navigate to="/queue" replace />} />
              </Routes>
            </Layout>
          </BrowserRouter>
        </AuthenticatedTemplate>
      </QueryClientProvider>
    </MsalProvider>
  );
}
