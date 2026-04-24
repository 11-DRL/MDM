import React from 'react';
import ReactDOM from 'react-dom/client';
import App from './App';
import './index.css';
import { msalInstance, MOCK_MODE } from './api/mdmApi';
import { fabricHost } from './lib/fabricHost';

async function bootstrap() {
  // MSAL initialize + redirect handling — wymagane w msal-browser v3
  if (!MOCK_MODE && !fabricHost.isInsideFabric) {
    await msalInstance.initialize();
    await msalInstance.handleRedirectPromise();
  }

  ReactDOM.createRoot(document.getElementById('root')!).render(
    <React.StrictMode>
      <App />
    </React.StrictMode>,
  );
}

bootstrap();
