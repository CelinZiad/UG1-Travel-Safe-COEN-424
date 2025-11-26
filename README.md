# Travel Safe

Cloud-based safety analysis service for Montreal. Analyzes crime and accident data to provide area safety scores and travel recommendations.

## Architecture

```
CKAN APIs (Crimes, Accidents)
        |
        v
    Ingestion (fetch_ckan_resource.py)
        |
        v
    S3 Data Lake (/raw, /ref, /curated, /served)
        |
        v
    EMR Serverless (Spark ETL Jobs)
        |
        v
    DynamoDB (AreaScores, Areas)
        |
        v
    FastAPI + Hugging Face (REST API)
        |
        v
    Frontend (Leaflet Map)
```

## Project Structure

```
.
├── api/                    # FastAPI REST service
│   ├── main.py            # API endpoints
│   ├── ai_safety.py       # Hugging Face integration
│   ├── dynamo_loader.py   # DynamoDB data loader
│   └── requirements.txt
├── etl/                    # Spark ETL jobs
│   ├── 10_clean_normalize.py
│   ├── 20_join_areas.py
│   └── 30_compute_scores.py
├── frontend/               # Web UI
│   ├── index.html
│   ├── style.css
│   └── app.js
├── ingest/                 # Data ingestion
│   └── fetch_ckan_resource.py
├── configs/                # EMR job configurations
├── infra/                  # Infrastructure configs
├── diagrams/               # Architecture diagrams
├── Dockerfile
└── docker-compose.yml
```

## Quick Start

### 1. Data Ingestion
```bash
pip install -r ingest/requirements.txt
python ingest/fetch_ckan_resource.py \
    --api-base https://donnees.montreal.ca/api/3/action \
    --resource-id 0f6d2b4a-f2cd-4e54-8a0f-25a823cfcc2f \
    --bucket ug1-travel-safe-bucket \
    --prefix raw/crime \
    --dataset-name actes_criminels
```

### 2. Run ETL (EMR Serverless)
```bash
aws s3 cp etl/30_compute_scores.py s3://ug1-travel-safe-bucket/code/etl/

aws emr-serverless start-job-run \
    --region ca-central-1 \
    --application-id 00g0msfb7tko833d \
    --execution-role-arn arn:aws:iam::358461553153:role/TravelSafe-EMR-ExecRole \
    --job-driver file://configs/jobdriver_compute_scores.json \
    --configuration-overrides file://configs/config_overrides.json
```

### 3. Load to DynamoDB
```bash
pip install -r api/requirements.txt
python api/dynamo_loader.py --create-tables
python api/dynamo_loader.py --bucket ug1-travel-safe-bucket --scores-prefix served/scores/json/
```

### 4. Run API
```bash
cd api && uvicorn main:app --reload --port 8000
```

### 5. Run Frontend
Open `frontend/index.html` in a browser or serve with:
```bash
cd frontend && python -m http.server 3000
```

## Docker Deployment

```bash
docker-compose up --build
```

- API: http://localhost:8000
- Frontend: http://localhost:3000

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /areas` | List all areas |
| `GET /areas/{id}` | Area details + latest score |
| `GET /areas/{id}/scores?from=YYYY-MM&to=YYYY-MM` | Historical scores |
| `GET /scores/latest` | Latest scores for all areas |
| `GET /advice/{id}` | AI safety advice |
| `POST /analyze-route` | Route safety analysis |
| `POST /ask?question=...` | Natural language queries |

## Diagram Generation

```bash
npm i -g @mermaid-js/mermaid-cli
mmdc -i diagrams/architecture.mmd -o diagrams/architecture.svg
```

## Team

- Elias Senoune (40248793)
- Mickel Samuel (40246743)
- Celine Ziade (40251642)
