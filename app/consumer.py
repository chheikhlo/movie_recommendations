from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
from pyspark.ml.recommendation import ALSModel
import streamlit as st
import pandas as pd
import altair as alt
import time
from threading import Thread
from pymongo import MongoClient

# Configuration de Streamlit
st.title("Real time movie predictions")

# Création de la session Spark
spark = SparkSession.builder \
    .appName("KafkaStreamlitApp") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/kafka_streamlit_checkpoints") \
    .getOrCreate()

# Schéma des données Kafka
schema = StructType([
    StructField("userId", IntegerType(), True),
    StructField("movieId", IntegerType(), True),
    StructField("rating", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Lire les données depuis Kafka
consumer = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "movielens_ratings") \
    .load()

# Extraire les valeurs de Kafka et les convertir en DataFrame
parsed_stream = consumer.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Charger le modèle ALS sauvegardé
als_model = ALSModel.load("hdfs://namenode:9000/model/als_model")

# Appliquer les prédictions en temps réel
predictions = als_model.transform(parsed_stream.select("userId", "movieId"))

# Stockage global des données pour Streamlit
predictions_storage = pd.DataFrame(columns=["userId", "movieId", "prediction"])

# Fonction pour insérer les données dans MongoDB
def insert_into_mongo(df):
  # Connexion à MongoDB
  client = MongoClient("mongodb://root:root@mongo:27017/")
  db = client['movie']
  collection = db['recommendations']

  # Convertir le DataFrame Spark en Pandas
  pandas_df = df.toPandas()

  if not pandas_df.empty:
    # Insérer dans MongoDB
    data = pandas_df.to_dict(orient='records')
    collection.insert_many(data)

# Fonction pour traiter les prédictions et les ajouter à la liste
def process_and_store(batch_df, batch_id):
    global predictions_storage
    pandas_df = batch_df.toPandas()

    if not pandas_df.empty:
        # Ajouter les nouvelles données au stockage
        predictions_storage = pd.concat([predictions_storage, pandas_df[["userId", "movieId", "prediction"]]], ignore_index=True)
        # Limiter les données pour éviter la surcharge (par exemple, les 20 dernières prédictions)
        predictions_storage = predictions_storage.tail(20)

         # Insérer les nouvelles prédictions dans MongoDB
        insert_into_mongo(batch_df)

# Configurer le flux Spark pour traiter les données
def run_spark_stream():
    query = predictions.writeStream \
        .foreachBatch(process_and_store) \
        .outputMode("append") \
        .start()
    query.awaitTermination()

# Lancer Spark Streaming dans un thread séparé
thread = Thread(target=run_spark_stream, daemon=True)
thread.start()

# Boucle Streamlit pour afficher les données
chart = st.empty()  # Conteneur pour le graphique

while True:
    if not predictions_storage.empty:
        # Grouper les données pour les afficher avec userId comme légende
        chart_data = predictions_storage.groupby(["userId", "movieId"]).mean().reset_index()

        # Afficher un graphique bar avec userId comme dimension
        chart.altair_chart(
            alt.Chart(chart_data).mark_bar().encode(
                x="movieId:O",
                y="prediction:Q",
                color="userId:N"
            ).properties(width=800, height=400)
        )

    time.sleep(2)  # Rafraîchissement toutes les 2 secondes
