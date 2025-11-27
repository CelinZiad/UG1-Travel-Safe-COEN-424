# Travel Safe Frontend

Interactive web map for visualizing Montreal borough safety scores.

## Quick Start (For Partner)

```bash
# 1. Make sure the API is running at localhost:8000
cd ../api && uvicorn main:app --reload

# 2. In another terminal, start the frontend
cd frontend
npm install
npm run dev

# 3. Open http://localhost:5173 in browser
```

## Features

- Interactive Map with real Montreal borough boundaries
- Safety Color Coding (Green = Safe, Yellow = Moderate, Red = Caution)
- Time-of-Day Selection (Morning/Afternoon/Night)
- AI Safety Advice for each borough
- Route Analysis for multiple boroughs
- Natural Language Questions

## Configuration

The frontend expects the API at `http://localhost:8000`. This is already configured in `.env`.

If needed, edit `.env`:
```
VITE_API_URL=http://localhost:8000
```

## API Requirements

Ensure the Travel Safe API is running with these endpoints:

- `GET /areas` - List boroughs with safety scores
- `GET /areas/{id}` - Single borough details
- `GET /advice/{id}` - AI safety advice
- `POST /analyze-route` - Multi-borough route analysis
- `POST /ask` - Natural language questions

## Project Structure

```
frontend/
├── public/
│   └── favicon.svg
├── src/
│   ├── assets/
│   │   ├── montreal-boroughs.js  # GeoJSON borough boundaries
│   │   └── styles.css            # Global styles
│   ├── composables/
│   │   └── useApi.js             # API client composable
│   ├── App.vue                   # Main application component
│   └── main.js                   # Vue app entry point
├── index.html
├── package.json
└── vite.config.js
```

## Usage

1. **View Safety Scores**: The map displays all boroughs colored by safety level
2. **Change Time Period**: Use the header buttons to switch between morning/afternoon/night
3. **Get Details**: Click a borough to see detailed scores and AI advice
4. **Plan Routes**: Add boroughs to your route and click "Analyze Route"
5. **Ask Questions**: Type safety questions to get AI-powered answers
