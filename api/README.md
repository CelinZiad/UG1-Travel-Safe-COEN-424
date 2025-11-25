# Travel Safe API

REST API service for Montreal area safety scoring.

## Setup

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure AWS Credentials
Ensure AWS credentials are configured with access to DynamoDB:
```bash
aws configure
```

### 3. Create DynamoDB Tables
```bash
python dynamo_loader.py --create-tables
```

### 4. Load Data
After running the Spark scoring job, load results into DynamoDB:
```bash
python dynamo_loader.py \
    --bucket ug1-travel-safe-bucket \
    --scores-prefix served/scores/json/
```

## Running the API

### Local Development
```bash
uvicorn main:app --reload --port 8000
```

### Docker
```bash
docker build -t travel-safe-api .
docker run -p 8000:8000 travel-safe-api
```

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | Service info |
| GET | `/health` | Health check |
| GET | `/areas` | List areas with pagination |
| GET | `/areas/{id}` | Get area details and latest score |
| GET | `/areas/{id}/scores` | Get historical scores for an area |
| GET | `/scores/latest` | Get latest scores for all areas |
| GET | `/advice/{id}` | Get AI safety advice for an area |
| POST | `/analyze-route` | Analyze safety of a multi-area route |
| POST | `/ask` | Ask a natural language safety question |

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `AWS_REGION` | `ca-central-1` | AWS region |
| `SCORES_TABLE` | `TravelSafe_AreaScores` | DynamoDB scores table |
| `AREAS_TABLE` | `TravelSafe_Areas` | DynamoDB areas table |
| `HF_MODEL` | `distilgpt2` | Hugging Face model for AI features |
