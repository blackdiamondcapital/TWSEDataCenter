# 權證雷達（Vue + Vite）

獨立前端，資料來自本專案 Flask API + Neon 權證表。

## 本機開發

1. 啟動後端（專案根目錄）：

```bash
python server.py
# 預設 http://127.0.0.1:5003
```

2. 啟動前端：

```bash
cd warrant-portal
npm install
npm run dev
```

瀏覽 http://127.0.0.1:5180 （Vite 會把 `/api` proxy 到 5003）

## 部署 Vercel

- Root Directory：`warrant-portal`
- Build：`npm run build`
- Output：`dist`
- Environment Variable：`VITE_API_BASE=https://你的後端網域/api`

後端 `ALLOWED_ORIGINS` 需加入 Vercel 網域。

## 功能

- 全市場主檔篩選（TWSE `tw_warrant_master` ∪ TPEX `tpex_warrant_master`）
- 當日成交熱度排行（金額／張數）
- 單檔走勢與詳情
- 同步最新 TWSE 成交
