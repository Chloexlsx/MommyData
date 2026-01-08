# MommyData - Maternal Health Data Explorer

An interactive data exploration website for pregnant or planning-to-be-pregnant individuals, using Australian Institute of Health and Welfare (AIHW) National Perinatal Data Collection data (2018-2023).

## Features

### Interactive Health Factor Analysis

- **Diabetes & Hypertension Trends**
  - Select your age and compare with historical data
  - View trends over years (2018-2023) for different age groups
  - Multiple sub-group selection (Pre-existing, Gestational, etc.)
  - Visual comparison with your age group highlighted

## Technical Stack

- **Backend**: FastAPI + SQLModel + SQLite
- **Frontend**: Jinja2 Templates + TailwindCSS + Chart.js
- **Design System**: Vibecamp (black & white style, Kalam/Inter fonts)

## Installation & Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Import Data

```bash
python scripts/import_data.py
```

This will import the diabetes and hypertension data from the CSV file in the `data/` directory.

### 3. Run the Application

```bash
uvicorn main:app --reload
```

The application will be available at http://localhost:8000

## Project Structure

```
MommyData/
├── app/
│   ├── models/          # Database models
│   ├── controllers/     # Business logic layer
│   ├── routes/          # API and web routes
│   ├── templates/       # Jinja2 templates
│   ├── static/          # Static resources
│   └── utils/           # Utility functions
├── scripts/
│   └── import_data.py   # Data import script
├── data/                 # Raw data files
└── main.py             # FastAPI application entry point
```

## API Endpoints

- `GET /api/v1/factor/{factor_name}` - Get factor data with sub-group disaggregation (diabetes/hypertension)
  - Query parameters: `age_group`, `start_year`, `end_year`, `sub_group` (multiple)
- `GET /api/v1/factor/{factor_name}/simple` - Get simplified factor data without sub-group disaggregation

## Data Source

Data from Australian Institute of Health and Welfare (AIHW) National Perinatal Data Collection annual update data visualization (2018-2023).

## License

A Vibecamp Creation
