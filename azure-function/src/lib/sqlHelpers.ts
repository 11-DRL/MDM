// Shared SQL helpers for Azure Function — used by mdmWrite (v1) and v2Routes.
// Single token cache and connection factory shared across all modules.

import { DefaultAzureCredential, type AccessToken } from '@azure/identity';
import * as sql from 'tedious';

const SQL_SCOPE = 'https://database.windows.net/.default';
const credential = new DefaultAzureCredential();
let cachedToken: AccessToken | null = null;

export async function getSqlAccessToken(): Promise<string> {
  const now = Date.now();
  if (cachedToken && cachedToken.expiresOnTimestamp - now > 5 * 60 * 1000) {
    return cachedToken.token;
  }
  const token = await credential.getToken(SQL_SCOPE);
  if (!token) throw new Error('Failed to acquire Azure AD token for Fabric SQL');
  cachedToken = token;
  return token.token;
}

export async function getConnection(): Promise<sql.Connection> {
  const token = await getSqlAccessToken();
  const config: sql.ConnectionConfiguration = {
    server: process.env.FABRIC_SQL_SERVER!,
    authentication: { type: 'azure-active-directory-access-token', options: { token } },
    options: {
      database: process.env.FABRIC_DATABASE ?? 'lh_mdm',
      encrypt: true,
      port: 1433,
      trustServerCertificate: false,
      connectTimeout: 30_000,
      requestTimeout: 60_000,
    },
  };
  return new sql.Connection(config);
}

export function sqlTypeForValue(value: unknown) {
  if (typeof value === 'bigint') return sql.TYPES.BigInt;
  if (typeof value === 'number') {
    if (!Number.isInteger(value)) return sql.TYPES.Float;
    return (value > 2147483647 || value < -2147483648) ? sql.TYPES.BigInt : sql.TYPES.Int;
  }
  if (typeof value === 'boolean') return sql.TYPES.Bit;
  if (value instanceof Date) return sql.TYPES.DateTime2;
  if (Buffer.isBuffer(value)) return sql.TYPES.VarBinary;
  return sql.TYPES.NVarChar;
}

export function addSqlParameters(req: sql.Request, params: Record<string, unknown>) {
  for (const [name, value] of Object.entries(params)) {
    req.addParameter(name, sqlTypeForValue(value), value as never);
  }
}

export async function withRetry<T>(fn: () => Promise<T>, attempts = 3): Promise<T> {
  let lastError: unknown;
  for (let i = 0; i < attempts; i++) {
    try {
      return await fn();
    } catch (err) {
      lastError = err;
      if (i < attempts - 1) {
        await new Promise(resolve => setTimeout(resolve, 200 * Math.pow(2, i)));
      }
    }
  }
  throw lastError;
}

export async function execSql(query: string, params: Record<string, unknown> = {}): Promise<void> {
  return withRetry(() => new Promise(async (resolve, reject) => {
    let conn: sql.Connection;
    try {
      conn = await getConnection();
    } catch (err) {
      return reject(err);
    }
    conn.on('connect', (err) => {
      if (err) return reject(err);

      const request = new sql.Request(query, (requestErr) => {
        conn.close();
        if (requestErr) reject(requestErr);
        else resolve();
      });

      addSqlParameters(request, params);
      conn.execSql(request);
    });
    conn.connect();
  }));
}

export async function execSqlWithRowCount(query: string, params: Record<string, unknown> = {}): Promise<number> {
  return withRetry(() => new Promise<number>(async (resolve, reject) => {
    let conn: sql.Connection;
    try {
      conn = await getConnection();
    } catch (err) {
      return reject(err);
    }
    conn.on('connect', (err) => {
      if (err) return reject(err);

      const request = new sql.Request(query, (requestErr, rowCount) => {
        conn.close();
        if (requestErr) reject(requestErr);
        else resolve(rowCount ?? 0);
      });

      addSqlParameters(request, params);
      conn.execSql(request);
    });
    conn.connect();
  }));
}

export async function querySql<T = Record<string, unknown>>(query: string, params: Record<string, unknown> = {}): Promise<T[]> {
  return withRetry(() => new Promise(async (resolve, reject) => {
    let conn: sql.Connection;
    try {
      conn = await getConnection();
    } catch (err) {
      return reject(err);
    }
    const rows: T[] = [];

    conn.on('connect', (err) => {
      if (err) return reject(err);

      const request = new sql.Request(query, (requestErr) => {
        conn.close();
        if (requestErr) reject(requestErr);
        else resolve(rows);
      });

      request.on('row', (columns: Array<{ metadata: { colName: string }; value: unknown }>) => {
        const row: Record<string, unknown> = {};
        for (const col of columns) {
          row[col.metadata.colName] = col.value;
        }
        rows.push(row as T);
      });

      addSqlParameters(request, params);
      conn.execSql(request);
    });
    conn.connect();
  }));
}
