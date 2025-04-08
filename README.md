# 📄 ETL Books Pipeline with Apache Airflow

This project is a **production-style ETL pipeline** that pulls book data from the Google Books API for 10 genres, cleans and deduplicates it, enriches it with metadata, and loads it into a **SQLite database**. The pipeline is orchestrated using **Apache Airflow** and runs daily at 2:15 PM UTC.

---

## 🔮 Key Features

- **Extract**: Fetches \~2000 book records from the Google Books API across 10 genres
- **Transform**: Cleans, normalizes, deduplicates records using fuzzy matching
- **Load**: Inserts or upserts cleaned data into a SQLite database
- **Orchestrate**: Fully managed by Airflow with custom DAG
- **Scheduled Run**: Executes daily using cron expression
- **Audit Logs**: Tracks last run timestamp and number of new records inserted

---

## 📊 ETL Architecture
![ETL Architecture](image.png)

---

## 📖 Genres Tracked

- Fiction
- Non-Fiction
- Business
- Psychology
- Science
- History
- Literature
- Children
- Romance
- Fantasy

---

## 🤠 Technologies Used

| Layer           | Tool                      |
| --------------- | ------------------------- |
| Language        | Python 3.11               |
| Orchestration   | Apache Airflow            |
| Database        | SQLite (PostgreSQL-ready) |
| Task Scheduling | Cron-based DAG            |
| Data Storage    | CSV + JSON Files          |
| Data Quality    | RapidFuzz, Pandas         |

---

## ⚖️ Data Quality Enhancements

- Title casing for book titles and authors
- Genre inference from filenames
- Duplicate detection using fuzzy title + author matching
- Only valid ISBNs stored

---

## ⌚ Scheduled Run

- **Time**: Every day at 2:15 PM UTC
- **DAG ID**: `book_etl_pipeline`
- **Trigger**: Manual or scheduled

---

## 🔔 Future Improvements

- Email/Slack alerts on failure
- Replace SQLite with PostgreSQL
- Add a front-end dashboard (Streamlit or Dash)
- Auto-detect and skip already fetched pages via hash tracking
- Add tests for cleaning and duplicate detection

---

## 📦 Setup Instructions

1. Clone the repo inside WSL/Linux:

```bash
git clone https://github.com/Swathi12499/ETL_books.git
```

2. Create and activate a virtual environment:

```bash
python3 -m venv airflow_env
source airflow_env/bin/activate
```

3. Install dependencies:

```bash
pip install -r etl/requirements.txt
```

4. Initialize Airflow:

```bash
airflow db init
airflow users create --username admin --password admin --firstname Swathi --lastname Manoharan --role Admin --email your@email.com
```

5. Start scheduler and webserver:

```bash
airflow scheduler
# in new terminal
airflow webserver --port 8080
```

6. Visit: [http://localhost:8080](http://localhost:8080) and trigger the DAG.

---

## 🚀 Author

**Swathi Manoharan**\
Engineer | Data Science 

---

> "Building systems that think, adapt, and run themselves."
