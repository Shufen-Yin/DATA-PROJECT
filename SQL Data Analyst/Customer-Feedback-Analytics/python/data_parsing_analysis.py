import csv
import json
import xml.etree.ElementTree as ET
import sqlite3
from datetime import datetime
import os

# ========= PATHS =========
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # path to python folder
DATA_DIR = os.path.join(BASE_DIR, "../data")
DB_PATH = os.path.join(BASE_DIR, "ecommerce_feedback.db")

CSV_FILE = os.path.join(DATA_DIR, "customer_survey.csv")
JSON_FILE = os.path.join(DATA_DIR, "web_feedback.json")
XML_FILE = os.path.join(DATA_DIR, "external_reviews.xml")
ERROR_LOG_FILE = os.path.join(BASE_DIR, "error_log.txt")

# ========= ERROR LOG FUNCTION =========
def log_error(source, record_id, error_msg):
    with open(ERROR_LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{source}] Record {record_id}: {error_msg}\n")

# ========= DATABASE CONNECTION =========
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# ========= CREATE STAGING TABLES =========
cur.executescript("""
DROP TABLE IF EXISTS survey_staging;
DROP TABLE IF EXISTS web_staging;
DROP TABLE IF EXISTS external_staging;

CREATE TABLE survey_staging (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id TEXT,
    rating INTEGER,
    comments TEXT,
    review_date TEXT
);

CREATE TABLE web_staging (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id TEXT,
    rating INTEGER,
    comments TEXT,
    review_date TEXT
);

CREATE TABLE external_staging (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id TEXT,
    rating INTEGER,
    comments TEXT,
    review_date TEXT
);
""")
conn.commit()

# ========= PARSE CSV (Survey) =========
with open(CSV_FILE, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for i, row in enumerate(reader, start=1):
        try:
            if not row["customer_id"] or not row["rating"]:
                log_error("CSV", i, "Missing required field(s)")
                continue
            rating = int(row["rating"])
            if rating < 1 or rating > 5:
                log_error("CSV", i, f"Invalid rating {rating}")
                continue
            # Validate date
            try:
                datetime.strptime(row["review_date"], "%d-%m-%Y")
            except:
                log_error("CSV", i, f"Invalid date {row['review_date']}")
                continue

            cur.execute(
                "INSERT INTO survey_staging (customer_id, rating, comments, review_date) VALUES (?, ?, ?, ?)",
                (row["customer_id"], rating, row["comments"], row["review_date"])
            )
        except Exception as e:
            log_error("CSV", i, str(e))

# ========= PARSE JSON (Web Feedback) =========
with open(JSON_FILE, "r", encoding="utf-8") as f:
    data = json.load(f)
    for i, record in enumerate(data, start=1):
        try:
            customer_id = record.get("customer_id")
            rating = int(record.get("rating", 0))
            comments = record.get("comments", "")
            review_date = record.get("review_date", "")

            if not customer_id or rating == 0:
                log_error("JSON", i, "Missing customer_id or rating")
                continue
            if rating < 1 or rating > 5:
                log_error("JSON", i, f"Invalid rating {rating}")
                continue
            if review_date:
                try:
                    datetime.strptime(review_date, "%Y-%m-%d")
                except:
                    log_error("JSON", i, f"Invalid date {review_date}")
                    continue

            cur.execute(
                "INSERT INTO web_staging (customer_id, rating, comments, review_date) VALUES (?, ?, ?, ?)",
                (customer_id, rating, comments, review_date)
            )
        except Exception as e:
            log_error("JSON", i, str(e))

# ========= PARSE XML (External Reviews) =========
tree = ET.parse(XML_FILE)
root = tree.getroot()

for i, review in enumerate(root.findall("review"), start=1):
    try:
        customer_id = review.find("customer_id").text if review.find("customer_id") is not None else None
        rating = int(review.find("rating").text) if review.find("rating") is not None else 0
        comments = review.find("comments").text if review.find("comments") is not None else ""
        review_date = review.find("review_date").text if review.find("review_date") is not None else ""

        if not customer_id or rating == 0:
            log_error("XML", i, "Missing customer_id or rating")
            continue
        if rating < 1 or rating > 5:
            log_error("XML", i, f"Invalid rating {rating}")
            continue
        if review_date:
            try:
                datetime.strptime(review_date, "%Y-%m-%d")
            except:
                log_error("XML", i, f"Invalid date {review_date}")
                continue

        cur.execute(
            "INSERT INTO external_staging (customer_id, rating, comments, review_date) VALUES (?, ?, ?, ?)",
            (customer_id, rating, comments, review_date)
        )
    except Exception as e:
        log_error("XML", i, str(e))

conn.commit()
print("✅ Data parsing completed. Check error_log.txt for issues.")

# ========= ANALYSIS QUERIES =========
print("\n--- Analysis Results ---")

# Top 10 customers by average rating
cur.execute("""
SELECT customer_id, AVG(rating) AS avg_rating, COUNT(*) AS review_count
FROM (
    SELECT * FROM survey_staging
    UNION ALL
    SELECT * FROM web_staging
    UNION ALL
    SELECT * FROM external_staging
)
GROUP BY customer_id
ORDER BY avg_rating DESC
LIMIT 10;
""")
for row in cur.fetchall():
    print(f"Top customer: {row}")

# Common complaint keywords
keywords = ["damaged", "delay", "defective", "late"]
for kw in keywords:
    cur.execute("""
    SELECT COUNT(*) FROM (
        SELECT comments FROM survey_staging
        UNION ALL
        SELECT comments FROM web_staging
        UNION ALL
        SELECT comments FROM external_staging
    ) WHERE comments LIKE ?;
    """, (f"%{kw}%",))
    count = cur.fetchone()[0]
    print(f"Keyword '{kw}' occurrences: {count}")

# Sentiment classification
cur.execute("""
SELECT customer_id, rating,
    CASE WHEN rating >= 4 THEN 'positive'
         WHEN rating = 3 THEN 'neutral'
         ELSE 'negative' END AS sentiment
FROM (
    SELECT * FROM survey_staging
    UNION ALL
    SELECT * FROM web_staging
    UNION ALL
    SELECT * FROM external_staging
);
""")
for row in cur.fetchall():
    print(f"Sentiment: {row}")

# Ratings trend by month
cur.execute("""
SELECT substr(review_date,1,7) AS month, AVG(rating) AS avg_rating, COUNT(*) AS review_count
FROM (
    SELECT * FROM survey_staging
    UNION ALL
    SELECT * FROM web_staging
    UNION ALL
    SELECT * FROM external_staging
)
WHERE review_date IS NOT NULL
GROUP BY month
ORDER BY month;
""")
for row in cur.fetchall():
    print(f"Monthly trend: {row}")

conn.close()
print("\n✅ Analysis completed.")
