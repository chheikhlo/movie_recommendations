from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
from pyspark.ml.recommendation import ALSModel
import streamlit as st
import pandas as pd

# Configuration de Streamlit
st.title("Prédictions de films en temps réel")
st.markdown("Affichage des prédictions des films basées sur les données Kafka et le modèle ALS.")

# Création de la session Spark
spark = SparkSession.builder \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints") \
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

# Fonction pour traiter et afficher les prédictions dans Streamlit
def process_and_visualize(batch_df, batch_id):
    # Convertir le batch Spark DataFrame en Pandas DataFrame
    pandas_df = batch_df.toPandas()

    # Affichage dans Streamlit
    if not pandas_df.empty:
        st.dataframe(pandas_df)
        st.bar_chart(pandas_df.set_index("movieId")["prediction"])

# Configurer le flux pour utiliser la fonction de visualisation
query = predictions.writeStream \
    .foreachBatch(process_and_visualize) \
    .outputMode("append") \
    .start()

# Attendre la fin du streaming
query.awaitTermination()
  