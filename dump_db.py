import pymysql
import os

db_host = os.environ.get('DB_HOST', 'localhost')
db_port = int(os.environ.get('DB_PORT', 3306))
db_user = os.environ.get('DB_USER', 'ingest')
db_pass = os.environ.get('DB_PASSWORD', 'ingestpw')
db_name = os.environ.get('DB_NAME', 'lseg')

conn = pymysql.connect(host=db_host, port=db_port, user=db_user, password=db_pass, database=db_name)
try:
    with conn.cursor() as cur:
        print("--- lseg_jobs ---")
        cur.execute("SELECT id, status, error_message FROM lseg_jobs ORDER BY id DESC LIMIT 5")
        for row in cur.fetchall():
            print(row)
        
        print("\n--- lseg_file_audit ---")
        cur.execute("SELECT file_name, status, error_message FROM lseg_file_audit")
        for row in cur.fetchall():
            print(row)

    print("\n--- Files in test-input ---")
    test_input_dir = "/Users/sa/Downloads/LSEG/lseg-ingest/test-input"
    if os.path.exists(test_input_dir):
        print(os.listdir(test_input_dir))
    else:
        print(f"{test_input_dir} does not exist")
finally:
    conn.close()
