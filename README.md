# Medical Telegram Data Warehouse

## Overview

This project builds a robust, production-ready data pipeline for collecting, storing, and analyzing medical-related Telegram messages. It is designed for reliability, scalability, and ease of use, following a strict multi-step challenge specification.

## Task 1: Telegram Scraping & Data Lake

### Features

- **Telegram Scraper**: Uses Telethon to collect messages, images, and metadata from specified Telegram channels.
- **Data Lake**: Stores raw messages and images in a structured directory under `data/raw/telegram_messages/` and `data/raw/images/`.
- **Logging**: Scraping operations are logged in `logs/` for traceability.
- **Loader Script**: `scripts/load_to_postgres.py` loads raw JSON data into a PostgreSQL table (`raw.telegram_messages`), creating the table if needed.
- **Configurable**: Channel list and credentials managed via `channels.json` and environment variables.

### Usage

1. **Configure Channels**: Edit `channels.json` to specify Telegram channels.
2. **Run Scraper**:
   ```bash
   python src/scraper.py
   ```
3. **Load Data to PostgreSQL**:
   ```bash
   python scripts/load_to_postgres.py
   ```

## Task 2: Data Modeling & Transformation (dbt)

### Features

- **dbt Project**: Located in `medical_warehouse/`, models raw data into a star schema for analytics.
- **Staging Model**: Deduplicates messages by `(channel_name, message_id)`.
- **Mart Models**:
  - `dim_channels`: Channel dimension.
  - `dim_dates`: Date dimension.
  - `fct_messages`: Fact table for messages.
- **Tests**: Comprehensive dbt tests for not-null, uniqueness (using dbt-utils for composite keys), relationships, and custom logic (no future dates, positive views).
- **Documentation**: Auto-generated with `dbt docs generate`.
- **CI Compatible**: All tests pass, ready for integration.

### Usage

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   cd medical_warehouse
   dbt deps
   ```
2. **Configure dbt Profile**: Edit `medical_warehouse/profiles.yml` with your PostgreSQL credentials.
3. **Run dbt Models**:
   ```bash
   dbt run --project-dir medical_warehouse --profiles-dir medical_warehouse
   dbt test --project-dir medical_warehouse --profiles-dir medical_warehouse
   dbt docs generate --project-dir medical_warehouse --profiles-dir medical_warehouse
   ```
4. **View Documentation**: Open `medical_warehouse/target/index.html` in your browser.

## Project Structure

- `src/`: Telegram scraper
- `scripts/`: Loader script
- `data/raw/`: Data lake (messages, images)
- `logs/`: Scraping and pipeline logs
- `medical_warehouse/`: dbt project (models, tests, docs)
- `requirements.txt`: Python dependencies

## Star Schema

- **Fact Table**: `fct_messages` (message_id, channel_key, date_key, metrics)
- **Dimensions**: `dim_channels` (channel info), `dim_dates` (date info)

## Notes

- All code and models are production-ready and validated.
- For CI, ensure environment variables and dbt profiles are set correctly.
- For troubleshooting, check logs in `logs/` and dbt test results.

## License

See `LICENSE` for details.

# medical-telegram-warehouse
