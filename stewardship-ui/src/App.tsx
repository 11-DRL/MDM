import { BrowserRouter, Routes, Route, NavLink, Navigate } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { MsalProvider, AuthenticatedTemplate, UnauthenticatedTemplate } from '@azure/msal-react';
import { Toaster } from 'sonner';
import { msalInstance, MOCK_MODE, API_SCOPE } from './api/mdmApi';
import { fabricHost } from './lib/fabricHost';
import { ReviewQueue } from './components/ReviewQueue/ReviewQueue';
import { PairDetail } from './components/PairDetail/PairDetail';
import { GoldenViewer } from './components/GoldenViewer/GoldenViewer';
import { GoldenList } from './components/GoldenList/GoldenList';
import { NewLocationForm } from './components/NewLocationForm/NewLocationForm';
import { ConfigViewer } from './components/ConfigViewer/ConfigViewer';
import { NotFound } from './components/NotFound/NotFound';
import { EntitySelector } from './components/EntitySelector/EntitySelector';
import { EntityForm } from './components/EntityForm/EntityForm';
import { EntityProvider, useEntity } from './hooks/useEntity';
import { LayoutDashboard, ListChecks, MapPin, Settings, PlusCircle } from 'lucide-react';
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
  const { selectedEntity } = useEntity();
  const hasMatching = selectedEntity?.hasMatching ?? true;

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
              <p className="text-xs text-gray-400">Stewardship</p>
            </div>
          </div>
        </div>

        {/* Entity selector */}
        <EntitySelector />

        <div className="my-2 border-t border-gray-100" />

        {/* Navigation — conditional on entity capabilities */}
        {hasMatching && (
          <NavItem to="/queue" label="Review Queue" icon={<ListChecks size={16} />} />
        )}
        <NavItem to="/golden" label="Golden Records" icon={<MapPin size={16} />} />
        <NavItem to="/config" label="Konfiguracja" icon={<Settings size={16} />} />
        {/* Separator */}
        <div className="mt-2 pt-2 border-t border-gray-100">
          <NavLink to="/new" className={({ isActive }) =>
            cn("flex items-center gap-2 px-3 py-2 rounded-lg text-sm transition-colors",
               isActive ? "bg-blue-50 text-blue-700 font-semibold"
                        : "text-blue-600 hover:bg-blue-50 font-medium")
          }>
            <PlusCircle size={16} />
            Nowy rekord
          </NavLink>
        </div>
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
        <p className="text-gray-400 text-sm mb-6">Konsolidacja danych referencyjnych</p>
        <button
          onClick={() => msalInstance.loginRedirect({ scopes: [API_SCOPE] })}
          className="w-full bg-blue-600 hover:bg-blue-700 text-white font-semibold py-2.5 px-4 rounded-xl transition-colors"
        >
          Zaloguj przez Azure AD
        </button>
      </div>
    </div>
  );
}

export default function App() {
  const appRoutes = (
    <>
      <Route path="/" element={<Navigate to="/queue" replace />} />
      {/* V1 legacy routes (business_location) */}
      <Route path="/queue"              element={<ReviewQueue />} />
      <Route path="/pairs/:pairId"      element={<PairDetail />} />
      <Route path="/golden"             element={<GoldenList />} />
      <Route path="/golden/:locationHk" element={<GoldenViewer />} />
      <Route path="/config"             element={<ConfigViewer />} />
      <Route path="/locations/new"      element={<NewLocationForm />} />
      {/* V2 generic entity form */}
      <Route path="/new"                element={<EntityForm />} />
      <Route path="*" element={<NotFound />} />
    </>
  );

  // Tryb 1: Mock — local dev bez żadnej konfiguracji
  if (MOCK_MODE) {
    return (
      <QueryClientProvider client={queryClient}>
        <EntityProvider>
          <Toaster position="top-right" richColors closeButton />
          <BrowserRouter>
            <Layout>
              <Routes>{appRoutes}</Routes>
            </Layout>
          </BrowserRouter>
        </EntityProvider>
      </QueryClientProvider>
    );
  }

  // Tryb 2: Fabric iFrame — token z hosta, brak potrzeby MSAL login
  if (fabricHost.isInsideFabric) {
    return (
      <QueryClientProvider client={queryClient}>
        <EntityProvider>
          <Toaster position="top-right" richColors closeButton />
          <BrowserRouter>
            <Layout>
              <Routes>{appRoutes}</Routes>
            </Layout>
          </BrowserRouter>
        </EntityProvider>
      </QueryClientProvider>
    );
  }

  // Tryb 3: Standalone — pełny Azure AD login przez MSAL
  return (
    <MsalProvider instance={msalInstance}>
      <QueryClientProvider client={queryClient}>
        <EntityProvider>
          <Toaster position="top-right" richColors closeButton />
          <UnauthenticatedTemplate>
            <LoginPage />
          </UnauthenticatedTemplate>
          <AuthenticatedTemplate>
            <BrowserRouter>
              <Layout>
                <Routes>{appRoutes}</Routes>
              </Layout>
            </BrowserRouter>
          </AuthenticatedTemplate>
        </EntityProvider>
      </QueryClientProvider>
    </MsalProvider>
  );
}
