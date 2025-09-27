## For Setup
1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Create a `.env` file (copy from `.env.example`) and update values:
   ```
   API_BASE_URL=https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets/microclimate-sensors-data/records
   PAGE_SIZE=500
   ```

3. Run the script:
   ```bash
   python main.py
   ```

4. After running the script, a local SQLite database `bloomeroo.db` should be created.
   You can query it using:
   ```bash
   sqlite3 bloomeroo.db
   SELECT COUNT(*) FROM sensor_readings;
   ```
