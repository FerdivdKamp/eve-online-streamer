# Market API analysis

Exploratory notebook for the mock `/api/market` endpoint. 
The first cell reads the repository's `openapi_specification.json` and writes a formatted copy to `analysis/openapi_specification_pretty.json`. 

The next three cells sample threshold behavior, timestamps, and pagination. They print observations and leave interpretation as TODOs.

## Setup

Start the mock API using the Docker instructions in the README of the project folder. It should be available at `http://localhost:8000`.

From this directory:

```powershell
poetry install
poetry run jupyter lab
```

Open `market_api_exploration.ipynb` and run its cells in order. Set `MARKET_API_BASE_URL` before starting Jupyter Lab if the API runs at another address. The API sampling cells need the mock service running; the specification cell works offline.


## Conclusion
The Swagger page suggests teh threshold and weekly comparison is already available which we greatly reduce the complexity of the project. However, a short analysis shows that the data appears to be mostly randomly generated, with a fixed itemid and regionid, but stationid and the rest may still differ, even when recalling pages from an api call.

We will create a python producer to provide the streaming data. We will add a timestamp since the API does not provide reliable data.