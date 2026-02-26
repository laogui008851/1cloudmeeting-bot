import urllib.request, json
from datetime import datetime

url = "https://meet.f13f2f75.org/api/admin-code?action=list&limit=500"
headers = {"User-Agent": "Mozilla/5.0", "Origin": "https://meet.f13f2f75.org"}
req = urllib.request.Request(url, headers=headers)
with urllib.request.urlopen(req, timeout=15) as r:
    data = json.loads(r.read().decode())

now = datetime.now().astimezone()
codes = data["codes"]
print(f"总in_use码数: {sum(1 for c in codes if int(c.get('in_use') or 0)==1)}")
print()
for c in codes:
    if int(c.get("in_use") or 0) != 1:
        continue
    ea = c.get("expires_at") or ""
    code = c["code"]
    if ea:
        try:
            exp = datetime.fromisoformat(str(ea).replace("Z","+00:00"))
            rem = exp - now
            secs = rem.total_seconds()
            if secs <= 0:
                h=int(abs(secs)//3600); m=int(abs(secs)%3600//60)
                print(f"[已过期] {code}  过期了{h}时{m}分前  expires_at={ea}")
            else:
                h=int(secs//3600); m=int(secs%3600//60)
                print(f"[使用中] {code}  剩余{h}时{m}分")
        except Exception as e:
            print(f"[解析失败] {code}  {e}")
    else:
        print(f"[无到期时间] {code}  expires_at=null")
