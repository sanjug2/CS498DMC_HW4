import os
from getpass import getpass
from flask import Flask, jsonify
from neo4j import GraphDatabase
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, round as spark_round

app = Flask(__name__)

spark = (
    SparkSession.builder
    .appName("TaxiAPI")
    .master("local[1]")
    .config("spark.driver.memory", "1g")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

password = os.environ.get("NEO4J_PASSWORD") or getpass("Neo4j password: ")

driver = GraphDatabase.driver(
        "bolt://localhost:7687",
        auth=("neo4j", password)
)

@app.route("/graph-summary", methods=["GET"])
def graph_summary():
    query = """
    MATCH (d:Driver)
    WITH count(d) AS driver_count
    MATCH (c:Company)
    WITH driver_count, count(c) AS company_count
    MATCH (a:Area)
    WITH driver_count, company_count, count(a) AS area_count
    MATCH ()-[t:TRIP]->()
    RETURN driver_count, company_count,
            area_count, count(t) AS trip_count
    """

    with driver.session() as session:
        record = session.run(query).single()

        return jsonify({
            "driver_count": record["driver_count"],
            "company_count": record["company_count"],
            "area_count": record["area_count"],
            "trip_count": record["trip_count"]
        })

@app.route("/top-companies", methods=["GET"])
def top_companies():
    from flask import request

    n = request.args.get("n", default=5, type=int)

    query = """
    MATCH (d:Driver)-[:WORKS_FOR]->(c:Company)
    MATCH (d)-[t:TRIP]->(:Area)
    RETURN c.name AS name, count(t) AS trip_count
    ORDER BY trip_count DESC, name ASC
    LIMIT $n
    """

    with driver.session() as session:
        records = session.run(query, n=max(n, 0))
        companies = [
                {
                    "name": record["name"],
                    "trip_count": record["trip_count"]
                }
                for record in records
        ]

    return jsonify({"companies": companies})

@app.route("/high-fare-trips", methods=["GET"])
def high_fare_trips():
    from flask import request

    area_id = request.args.get("area_id", type=int)
    min_fare = request.args.get("min_fare", type=float)

    if area_id is None or min_fare is None:
        return jsonify({"error": "Missing or invalid parameters"}), 400

    query = """
    MATCH (d:Driver)-[t:TRIP]->(a:Area)
    WHERE a.area_id = $area_id AND t.fare > $min_fare
    RETURN t.trip_id AS trip_id,
           t.fare AS fare,
           d.driver_id AS driver_id
    ORDER BY fare desc
    """

    with driver.session() as session:
        records = session.run(
                query, area_id=area_id, min_fare=min_fare
        )
        trips = [dict(record) for record in records]
    
    return jsonify({"trips": trips})

@app.route("/co-area-drivers", methods=["GET"])
def co_area_drivers():
    from flask import request

    driver_id = request.args.get("driver_id")

    if not driver_id:
        return jsonify({"error": "Missing driver_id"}), 400

    query = """
    MATCH (d1:Driver {driver_id: $driver_id})-[:TRIP]->(a:Area)
        <-[:TRIP]-(d2:Driver)
    WHERE d1 <> d2
    RETURN d2.driver_id AS driver_id,
           count(DISTINCT a) AS shared_areas
    ORDER BY shared_areas DESC, driver_id ASC
    """

    with driver.session() as session:
        records = session.run(query, driver_id=driver_id)
        results = [dict(record) for record in records]

    return jsonify({"co_area_drivers": results})

@app.route("/avg-fare-by-company", methods=["GET"])
def avg_fare_by_company():
    query = """
    MATCH (c:Company)<-[:WORKS_FOR]-(d:Driver)-[t:TRIP]->(a:Area)
    RETURN c.name AS name,
            round(avg(t.fare), 2) AS avg_fare
    ORDER BY avg_fare DESC, name ASC
    """

    with driver.session() as session:
        records = session.run(query)
        companies = [dict(record) for record in records]

    return jsonify({"companies": companies})

@app.route("/area-stats", methods=["GET"])
def area_stats():
    from flask import request

    area_id = request.args.get("area_id", type=int)
    if area_id is None:
        return jsonify({"error": "Missing area_id"}), 400

    df = spark.read.csv(
        "taxi_trips_clean.csv",
        header=True,
        inferSchema=True
    )

    filtered = df.filter(col("dropoff_area") == area_id)

    stats = filtered.groupBy("dropoff_area").agg(
        count("*").alias("trip_count"),
        spark_round(avg("fare"), 2).alias("avg_fare"),
        spark_round(avg("trip_seconds"), 0).alias("avg_trip_seconds")
    ).collect()

    if not stats:
        return jsonify({
            "area_id": area_id,
            "trip_count": 0,
            "avg_fare": None,
            "avg_trip_seconds": None
        })

    result = stats[0]

    return jsonify({
        "area_id": area_id,
        "trip_count": result["trip_count"],
        "avg_fare": result["avg_fare"],
        "avg_trip_seconds": int(result["avg_trip_seconds"])
    })

@app.route("/top-pickup-areas", methods=["GET"])
def top_pickup_areas():
    from flask import request

    n = request.args.get("n", default=5, type=int)

    if n < 1:
        return jsonify({"error": "n must be positive"}), 400

    df = spark.read.csv(
        "taxi_trips_clean.csv",
        header=True,
        inferSchema=True
    )

    results = (
        df.groupBy("pickup_area")
        .agg(count("*").alias("trip_count"))
        .orderBy(col("trip_count").desc(), col("pickup_area").asc())
        .limit(n)
        .collect()
    )

    areas = [
        {
            "pickup_area": row["pickup_area"],
            "trip_count": row["trip_count"]
        }
        for row in results
    ]

    return jsonify({"areas": areas})

@app.route("/company-compare", methods=["GET"])
def company_compare():
    from flask import request

    company1 = request.args.get("company1")
    company2 = request.args.get("company2")

    if not company1 or not company2:
        return jsonify({"error": "Missing company parameter"}), 400

    df = spark.read.csv(
        "taxi_trips_clean.csv",
        header=True,
        inferSchema=True
    )

    df = df.withColumn(
        "fare_per_minute",
        col("fare") / (col("trip_seconds") / 60.0)
    )

    df.createOrReplaceTempView("company_trips")

    query = """
        SELECT
            company,
            COUNT(*) AS trip_count,
            ROUND(AVG(fare), 2) AS avg_fare,
            ROUND(AVG(fare_per_minute), 2) AS avg_fare_per_minute,
            CAST(ROUND(AVG(trip_seconds), 0) AS INT)
                AS avg_trip_seconds
        FROM company_trips
        WHERE company IN (:company1, :company2)
        GROUP BY company
    """

    results = spark.sql(query, args={
        "company1": company1,
        "company2": company2
    }).collect()

    if len(results) != len({company1, company2}):
        return jsonify({
            "error": "one or more companies not found"
        })

    comparison = [row.asDict() for row in results]

    return jsonify({"comparison": comparison})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
