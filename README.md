# MommyData - NSW Mothers and Babies Data Explorer

一個針對懷孕或計劃懷孕人士的互動式資料探索網站，使用 NSW Health Mothers and Babies 2023 開放數據。

## 功能特色

### 三個主要場景

1. **我在準備懷孕**
   - 根據年齡、BMI、吸菸、糖尿病、高血壓等條件
   - 提供懷孕結果統計與風險評估

2. **我已懷孕**
   - 根據產檢開始週數、年齡等條件
   - 提供引產/剖腹率、早產/低體重、NICU 等統計

3. **我在選醫院**
   - 根據地區選擇
   - 比較不同醫院的分娩方式、疼痛緩解、住院天數、哺乳率等

## 技術架構

- **後端**: FastAPI + SQLModel + SQLite
- **前端**: Jinja2 Templates + TailwindCSS + Vanilla JavaScript
- **設計系統**: Vibecamp (黑白風格，Kalam/Inter 字體)

## 安裝與執行

### 1. 安裝依賴

```bash
pip install -r requirements.txt
```

### 2. 匯入數據

```bash
python scripts/import_data.py
```

### 3. 啟動應用程式

```bash
uvicorn main:app --reload
```

應用程式將在 http://localhost:8000 啟動

## 專案結構

```
MommyData/
├── app/
│   ├── models/          # 資料庫模型
│   ├── controllers/     # 業務邏輯層
│   ├── routes/          # API 路由
│   ├── templates/       # Jinja2 模板
│   ├── static/          # 靜態資源
│   └── utils/           # 工具函數
├── scripts/
│   └── import_data.py   # 數據匯入腳本
├── data/                 # 原始數據檔案
└── main.py             # FastAPI 應用入口
```

## API 端點

- `GET /api/v1/scenario/preparing` - 準備懷孕場景數據
- `GET /api/v1/scenario/pregnant` - 已懷孕場景數據
- `GET /api/v1/scenario/hospital` - 選醫院場景數據
- `GET /api/v1/compare` - 個人化對比數據

## 授權

A Vibecamp Creation

