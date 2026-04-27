// Apply T-SQL DDL to a Fabric Warehouse using user's Azure login token.
// Usage: node apply-warehouse-ddl.js <server> <database> <sqlFile> [--schemas=<json>]
//   --schemas: opcjonalnie JSON z mapą zamian, np. '{"SCHEMA_SILVER":"mdm_silver_dv","SCHEMA_GOLD":"gold"}'
//              Każdy {{KEY}} w pliku zostanie zastąpiony wartością.
//              Jeśli plik zawiera placeholdery a brak --schemas → błąd przed wykonaniem.
// Requires: az login; dependencies resolved from ../azure-function/node_modules

const path = require('path');
const fs = require('fs');
const { execSync } = require('child_process');

const TEDIOUS_PATH = path.resolve(__dirname, '..', 'azure-function', 'node_modules', 'tedious');
if (!fs.existsSync(TEDIOUS_PATH)) {
  console.error(`tedious not found at ${TEDIOUS_PATH}. Run 'npm install' in azure-function/.`);
  process.exit(2);
}
const tedious = require(TEDIOUS_PATH);

const args = process.argv.slice(2);
const positional = args.filter(a => !a.startsWith('--'));
const flags = Object.fromEntries(
  args.filter(a => a.startsWith('--')).map(a => {
    const [k, ...rest] = a.replace(/^--/, '').split('=');
    return [k, rest.join('=')];
  }),
);
const [server, database, sqlFile] = positional;
if (!server || !database || !sqlFile) {
  console.error('Usage: node apply-warehouse-ddl.js <server> <database> <sqlFile> [--schemas=<json>]');
  process.exit(2);
}

function getAzToken() {
  const out = execSync('az account get-access-token --resource "https://database.windows.net" --query accessToken -o tsv', { encoding: 'utf8' });
  return out.trim();
}

// Substitute {{KEY}} placeholders. Throws if any unresolved placeholder remains.
function applyTemplate(sqlText, schemas) {
  let out = sqlText;
  if (schemas) {
    for (const [k, v] of Object.entries(schemas)) {
      out = out.split(`{{${k}}}`).join(v);
    }
  }
  const remaining = out.match(/\{\{[A-Z_]+\}\}/g);
  if (remaining) {
    throw new Error(`Unresolved placeholders in ${sqlFile}: ${[...new Set(remaining)].join(', ')}. Pass --schemas with all keys.`);
  }
  return out;
}

// Split on "GO" lines OR on blank-line boundaries between IF/CREATE blocks.
// Simpler: split on ";\s*(\r?\n){1,}" — we already end every statement with ';'.
function splitBatches(sqlText) {
  // Remove line comments for safety in the splitter
  const stripped = sqlText.replace(/--[^\r\n]*/g, '');
  // Split on trailing semicolon at line end
  return stripped
    .split(/;\s*(?:\r?\n)+/)
    .map(s => s.trim())
    .filter(s => s.length > 0);
}

function runBatch(conn, batchSql) {
  return new Promise((resolve, reject) => {
    const req = new tedious.Request(batchSql, err => err ? reject(err) : resolve());
    conn.execSqlBatch(req);
  });
}

(async () => {
  const token = getAzToken();
  let sqlText = fs.readFileSync(sqlFile, 'utf8');
  if (flags.schemas) {
    const schemas = JSON.parse(flags.schemas);
    sqlText = applyTemplate(sqlText, schemas);
    console.log(`Applied template substitutions: ${Object.keys(schemas).join(', ')}`);
  } else if (sqlText.match(/\{\{[A-Z_]+\}\}/)) {
    // Plik ma placeholdery a my nie dostaliśmy --schemas → fail-fast (defaulty na poziomie callera).
    console.error(`File ${sqlFile} contains {{...}} placeholders but no --schemas flag was provided.`);
    process.exit(2);
  }
  const batches = splitBatches(sqlText);
  console.log(`Loaded ${batches.length} batch(es) from ${sqlFile}`);

  const conn = new tedious.Connection({
    server,
    authentication: { type: 'azure-active-directory-access-token', options: { token } },
    options: {
      database,
      encrypt: true,
      port: 1433,
      trustServerCertificate: false,
      connectTimeout: 60000,
      requestTimeout: 120000,
    },
  });

  await new Promise((resolve, reject) => {
    conn.on('connect', err => err ? reject(err) : resolve());
    conn.connect();
  });
  console.log(`Connected to ${server}/${database}`);

  let ok = 0, failed = 0;
  for (let i = 0; i < batches.length; i++) {
    const b = batches[i];
    const head = b.split('\n')[0].substring(0, 80);
    try {
      await runBatch(conn, b);
      ok++;
      process.stdout.write(`  [${i + 1}/${batches.length}] OK: ${head}\n`);
    } catch (err) {
      failed++;
      process.stdout.write(`  [${i + 1}/${batches.length}] FAIL: ${head}\n    -> ${err.message}\n`);
    }
  }

  conn.close();
  console.log(`Done. OK=${ok} FAIL=${failed}`);
  process.exit(failed > 0 ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
