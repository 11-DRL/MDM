// Quick T-SQL query runner (SELECT or statements). Prints rows to stdout.
// Usage: node sql-query.js <server> <database> <sqlText-or-@file>
const path = require('path');
const fs = require('fs');
const { execSync } = require('child_process');

const TEDIOUS_PATH = path.resolve(__dirname, '..', 'azure-function', 'node_modules', 'tedious');
const tedious = require(TEDIOUS_PATH);

const [, , server, database, arg] = process.argv;
if (!server || !database || !arg) {
  console.error('Usage: node sql-query.js <server> <database> <sqlText-or-@file>');
  process.exit(2);
}

const sqlText = arg.startsWith('@') ? fs.readFileSync(arg.substring(1), 'utf8') : arg;

function getAzToken() {
  return execSync('az account get-access-token --resource "https://database.windows.net" --query accessToken -o tsv', { encoding: 'utf8' }).trim();
}

(async () => {
  const token = getAzToken();
  const conn = new tedious.Connection({
    server, authentication: { type: 'azure-active-directory-access-token', options: { token } },
    options: { database, encrypt: true, port: 1433, connectTimeout: 60000, requestTimeout: 120000 },
  });
  await new Promise((res, rej) => { conn.on('connect', e => e ? rej(e) : res()); conn.connect(); });

  // Split into batches, like apply-warehouse-ddl.js
  const stripped = sqlText.replace(/--[^\r\n]*/g, '');
  const batches = stripped.split(/;\s*(?:\r?\n)+/).map(s => s.trim()).filter(s => s.length > 0);

  for (let i = 0; i < batches.length; i++) {
    const b = batches[i];
    const head = b.split('\n')[0].substring(0, 100);
    await new Promise((resolve) => {
      const req = new tedious.Request(b, (err, rowCount) => {
        if (err) console.log(`  [${i+1}/${batches.length}] FAIL: ${head}\n    -> ${err.message}`);
        else console.log(`  [${i+1}/${batches.length}] OK (rows=${rowCount}): ${head}`);
        resolve();
      });
      req.on('row', cols => {
        const row = cols.map(c => {
          const v = c.value;
          if (Buffer.isBuffer(v)) return '0x' + v.toString('hex').substring(0, 16) + '...';
          if (v instanceof Date) return v.toISOString();
          return String(v);
        }).join(' | ');
        console.log('    ' + row);
      });
      conn.execSql(req);
    });
  }

  conn.close();
  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
