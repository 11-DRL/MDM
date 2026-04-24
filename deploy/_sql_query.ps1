param([string]$server, [string]$database, [string]$query)
# Quick-ad-hoc T-SQL SELECT
$TEDIOUS = "c:\Users\LukaszLelwic\MDM\MDM\azure-function\node_modules\tedious"
$js = @"
const path='$TEDIOUS'.replace(/\\\\/g,'/');
const tedious=require(path);
const {execSync}=require('child_process');
const token=execSync('az account get-access-token --resource "https://database.windows.net" --query accessToken -o tsv',{encoding:'utf8'}).trim();
const conn=new tedious.Connection({server:'$server',authentication:{type:'azure-active-directory-access-token',options:{token}},options:{database:'$database',encrypt:true,port:1433,connectTimeout:60000,requestTimeout:120000}});
conn.on('connect',err=>{if(err){console.error(err);process.exit(1);}
  const req=new tedious.Request(process.argv[2],(e)=>{if(e){console.error('FAIL:',e.message);process.exit(1);} conn.close();});
  req.on('row',cols=>{console.log(cols.map(c=>c.value).join(' | '));});
  conn.execSql(req);
});
conn.connect();
"@
$tmp = [System.IO.Path]::GetTempFileName() + ".js"
$js | Set-Content $tmp -NoNewline
node $tmp $query
Remove-Item $tmp
