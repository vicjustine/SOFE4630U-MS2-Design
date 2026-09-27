"""
SOFE 4630U Milestone 2 - Design part: verify the MySQL storage stage.

The producer publishes to Pub/Sub and the Integration Connector writes to
MySQL, so nothing in the pipeline reports back whether the rows actually
arrived. This script performs that check: it connects to the MySQL server
deployed on GKE and reports what is stored in the SmartMeter table.

    python3 verify_mysql.py <mysql-external-ip>

It is the programmatic equivalent of logging into the MySQL CLI and running
"select * from SmartMeter", and it is the check step shown in the video.
"""

import sys

try:
    import pymysql   # pip install pymysql
except ModuleNotFoundError:
    raise SystemExit("The pymysql library is required.  Install it with:  pip install pymysql")

# ======= EDIT THIS =======
DB_USER = "usr"
DB_PASSWORD = "sofe4630u"     # set in mysql-deploy.yaml
DB_NAME = "Readings"
DB_PORT = 3306
TABLE = "SmartMeter"
# =========================


def main():
    if len(sys.argv) < 2:
        raise SystemExit(
            "Usage: python3 verify_mysql.py <mysql-external-ip>\n"
            "Get the IP with: kubectl get service mysql-service"
        )
    host = sys.argv[1]

    print(f"Connecting to MySQL at {host}:{DB_PORT} ...")
    conn = pymysql.connect(host=host, port=DB_PORT, user=DB_USER,
                           password=DB_PASSWORD, database=DB_NAME,
                           cursorclass=pymysql.cursors.DictCursor)

    with conn, conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) AS n FROM {TABLE}")
        total = cur.fetchone()["n"]
        print(f"\nRows currently in {TABLE}: {total}\n")

        if total == 0:
            print("The table is empty. Check that the integration is published")
            print("and that the connector is not suspended.")
            return

        # how many rows came from each profile
        cur.execute(f"SELECT profile_name, COUNT(*) AS n FROM {TABLE} "
                    f"GROUP BY profile_name ORDER BY n DESC")
        print("Records per profile:")
        for r in cur.fetchall():
            print(f"  {r['profile_name']:<10} {r['n']}")

        # rows where the sensor reading was missing in the CSV
        cur.execute(f"SELECT COUNT(*) AS n FROM {TABLE} WHERE humidity IS NULL")
        print(f"\nRows with a NULL humidity (missing in the source CSV): "
              f"{cur.fetchone()['n']}")

        # the most recently inserted rows
        cur.execute(f"SELECT ID, time, profile_name, temperature, humidity, pressure "
                    f"FROM {TABLE} ORDER BY ID DESC LIMIT 5")
        print("\nMost recent 5 rows:")
        for r in cur.fetchall():
            print(f"  ID={r['ID']:<6} {r['profile_name']:<8} "
                  f"temp={r['temperature']} humidity={r['humidity']} "
                  f"pressure={r['pressure']}")


if __name__ == "__main__":
    main()
