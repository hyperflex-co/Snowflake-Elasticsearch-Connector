# Snowflake → Elasticsearch Sync Service

A lightweight, production‑ready Python service that **incrementally syncs data from Snowflake into Elasticsearch** using a timestamp‑based high‑watermark strategy.

This connector is designed to be:

- **Simple** — minimal dependencies, clean architecture  
- **Reliable** — incremental sync with state tracking  
- **Config‑driven** — no hard‑coded credentials or table names  
- **Extensible** — easy to add transformations, enrichments, or new targets  

---

## Features

- **Incremental sync** using a timestamp column  
- **Snowflake ingestion** via `snowflake-connector-python`  
- **Elasticsearch indexing** using the official ES client  
- **State tracking** (`state/state.json`) to resume where you left off  
- **Config‑driven** (`config/config.yaml`)  
- **Automatic row conversion** (timestamps → ISO strings)  
- **Structured logging**  
- **Supports one‑shot or continuous sync**  

---

## Project Structure

```
snowflake_connector/
│
├── clients/
│   ├── elastic_client.py
│   └── snowflake_client.py
│
├── config/
│   └── config.example.yaml
│
├── services/
│   └── sync_service.py
│
├── state/
│   └── .gitkeep
│
├── utils/
│   ├── id_generation.py
│   ├── logging.py
│   └── row_conversion.py
│
├── main.py
├── requirements.txt
├── LICENSE.txt
└── .gitignore
```

---

## Installation

### 1. Clone the repo

```bash
git clone https://github.com/hyperflex-co/Snowflake-Elasticsearch-Connector.git
cd Snowflake-Elasticsearch-Connector
```

### 2. Create a virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

---

## Configuration

Copy the example config:

```bash
cp config/config.example.yaml config/config.yaml
```

Then edit `config/config.yaml` with your:

### Snowflake settings
- account  
- user  
- password or private key  
- warehouse  
- database  
- schema  
- role  
- table  
- timestamp column  

### Elasticsearch settings
- host  
- username/password or API key  
- index name  

### Service settings
- sync interval  
- state file path  
- log level  

---

# (Optional) Using Snowflake Key‑Pair Authentication

If you prefer **key‑pair authentication** instead of a password, you can generate a private key and register its public key with Snowflake.

### **1. Generate a private key**

```bash
openssl genrsa -out rsa_key.p8 2048
```

### **2. Convert it to PKCS8 format (required by Snowflake)**

```bash
openssl pkcs8 -topk8 -inform PEM -outform PEM \
  -in rsa_key.p8 -out rsa_key_pkcs8.p8 -nocrypt
```

### **3. Generate the public key**

```bash
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

### **4. Add the public key to your Snowflake user**

```sql
ALTER USER <your_username> SET RSA_PUBLIC_KEY='<contents of rsa_key.pub>';
```

### **5. Update your config**

```yaml
snowflake:
  user: "your_username"
  private_key: "path/to/rsa_key_pkcs8.p8"
  password: null
```

---

## Running the Sync Service

### **Run one sync cycle and exit**

```bash
python main.py --once
```

### **Run continuously (recommended for production)**

```bash
python main.py
```

### **Reset the state file (forces a full resync)**

```bash
python main.py --reset-state
```

---

## How Incremental Sync Works

The service tracks the **last successfully synced timestamp** in:

```
state/state.json
```

Example:

```json
{
  "last_timestamp": "2026-04-29T10:15:00"
}
```

Each sync cycle:

1. Reads the last timestamp  
2. Queries Snowflake for rows where `timestamp_column > last_timestamp`  
3. Converts rows into Elasticsearch‑safe documents  
4. Bulk indexes them into Elasticsearch  
5. Updates `state.json`  

If the file is missing or corrupted, the service automatically falls back to:

```
1970-01-01T00:00:00
```

---

## Architecture Overview

### **SnowflakeClient**
- Connects to Snowflake  
- Executes incremental queries  
- Normalizes timestamps  

### **ElasticClient**
- Connects to Elasticsearch  
- Creates index if needed  
- Performs bulk indexing  

### **SyncService**
- Loads state  
- Fetches new rows  
- Converts rows  
- Indexes documents  
- Saves updated state  

### **Utilities**
- `row_conversion.py` — cleans and converts Snowflake rows  
- `logging.py` — global logging setup  
- `id_generation.py` — optional ID helpers  

---

## Security Notes

- Never commit `config.yaml`  
- Never commit private keys  
- Never commit `state.json`  
- Use `.env` or environment variables in production  
- Prefer Snowflake key‑pair auth when possible  

Your `.gitignore` already protects you from accidental leaks.

---

## License

This project is licensed under the **Business Source License (BSL)**.  
See `LICENSE.txt` for full terms.

---
## Contributing

Pull requests are welcome.  
If you want help extending the connector (e.g., delete detection, schema evolution, backfill mode), feel free to open an issue.
