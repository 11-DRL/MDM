// Fabric Host Bridge — obsługuje token injection z Fabric iFrame host
// Gdy apka działa WEWNĄTRZ Fabric workspace, Fabric host wstrzykuje token
// przez window.postMessage zamiast MSAL. Ten moduł to abstrahuje.
//
// Źródło: https://learn.microsoft.com/fabric/extensibility-toolkit/architecture
// SDK:    @ms-fabric/workload-client (opcjonalne — tu minimalna implementacja)

type TokenListener = (token: string) => void;

interface FabricTokenMessage {
  type: 'FABRIC_AUTH_TOKEN';
  token: string;
  expiresAt: number;   // Unix timestamp ms
}

class FabricHostBridge {
  private token: string | null = null;
  private expiresAt = 0;
  private listeners: TokenListener[] = [];
  private initialized = false;

  /** Sprawdza czy działamy wewnątrz Fabric iFrame */
  get isInsideFabric(): boolean {
    try {
      return window.self !== window.top;
    } catch {
      return true;   // cross-origin — prawdopodobnie jesteśmy w iFrame
    }
  }

  init() {
    if (this.initialized) return;
    this.initialized = true;

    window.addEventListener('message', (event: MessageEvent) => {
      // Akceptuj wiadomości z Fabric (*.fabric.microsoft.com lub localhost w dev)
      const allowedOrigins = [
        'https://app.fabric.microsoft.com',
        'https://msit.powerbi.com',
        'http://localhost:3000',
        window.location.origin,
      ];
      if (!allowedOrigins.some(o => event.origin.startsWith(o.replace('localhost:3000', 'localhost')))) {
        return;
      }

      const msg = event.data as FabricTokenMessage;
      if (msg?.type === 'FABRIC_AUTH_TOKEN' && msg.token) {
        this.token = msg.token;
        this.expiresAt = msg.expiresAt;
        this.listeners.forEach(fn => fn(msg.token));
      }
    });

    // Powiadom Fabric hosta że jesteśmy gotowi na token
    if (this.isInsideFabric) {
      window.parent.postMessage({ type: 'WORKLOAD_READY' }, '*');
    }
  }

  /** Zwraca aktualny token lub null jeśli wygasł / niedostępny */
  getToken(): string | null {
    if (!this.token) return null;
    if (Date.now() > this.expiresAt - 30_000) return null;   // refresh 30s przed końcem
    return this.token;
  }

  /** Czeka na pierwszy token (max waitMs ms) */
  waitForToken(waitMs = 5000): Promise<string | null> {
    const current = this.getToken();
    if (current) return Promise.resolve(current);

    return new Promise(resolve => {
      const timer = setTimeout(() => {
        this.listeners = this.listeners.filter(fn => fn !== onToken);
        resolve(null);
      }, waitMs);

      const onToken = (token: string) => {
        clearTimeout(timer);
        this.listeners = this.listeners.filter(fn => fn !== onToken);
        resolve(token);
      };

      this.listeners.push(onToken);
    });
  }

  onTokenRefresh(fn: TokenListener) {
    this.listeners.push(fn);
    return () => { this.listeners = this.listeners.filter(l => l !== fn); };
  }
}

// Singleton
export const fabricHost = new FabricHostBridge();

// Inicjalizuj od razu przy imporcie
fabricHost.init();
