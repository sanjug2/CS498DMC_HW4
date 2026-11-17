import os
import pandas as pd
from getpass import getpass
from neo4j import GraphDatabase

df = pd.read_csv("taxi_trips_clean.csv")

password = os.environ.get("NEO4J_PASSWORD") or getpass("Neo4j password: ")

driver = GraphDatabase.driver(
        "bolt://localhost:7687",
        auth=("neo4j", password)
)

driver.verify_connectivity()

def load_trip(tx, row):
    tx.run("""
        MERGE (d:Driver {driver_id: $driver_id})
        MERGE (c:Company {name: $company})
        MERGE (a:Area {area_id: $dropoff_area})
        MERGE (d)-[:WORKS_FOR]->(c)
        MERGE (d)-[t:TRIP {trip_id: $trip_id}]->(a)
        SET t.fare = $fare,
            t.trip_seconds = $trip_seconds
    """,
        driver_id=str(row["driver_id"]),
        company=str(row["company"]),
        dropoff_area=int(row["dropoff_area"]),
        trip_id=str(row["trip_id"]),
        fare=float(row["fare"]),
        trip_seconds=int(row["trip_seconds"])
    )

try:
    with driver.session() as session:
        for i, row in df.iterrows():
            session.execute_write(load_trip, row)

            if (i+1) % 1000 == 0:
                print(f"Loaded {i + 1} trips")
        
        print("Graph loading complete!")
finally:
    driver.close()
