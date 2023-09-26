# Databricks notebook source
spark.conf.set(
    f"fs.azure.account.key.fatimazahraestorage.dfs.core.windows.net", 
    "sfzg4z+1EOSmh6JKWj2GEBMYN7D37oOytBVZQaNVRAnrDe5zn/tDGTpVRHeDRflOZCPds+vhzLVj+AStYBXUdA=="
)


# COMMAND ----------

dbutils.fs.ls("abfss://fatimazahraecontainer@fatimazahraestorage.dfs.core.windows.net/raw/")


# COMMAND ----------

file_location = "abfss://fatimazahraecontainer@fatimazahraestorage.dfs.core.windows.net/raw/"


# COMMAND ----------

df = spark.read.format("csv").option("inferSchema", "True").option("header",
"True").option("delimeter",",").load(file_location)

display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

from pyspark.sql.functions import when, expr, col
from pyspark.sql.functions import to_timestamp, date_format

# Remplacer "24" par "00" et "25" par "01" dans la colonne "ArrivalTime"
df = df.withColumn(
    "ArrivalTime",
    when(
        expr("substring(ArrivalTime, 1, 2) > 23"),
        expr("concat('00', substring(ArrivalTime, 3, 5))")
    ).otherwise(col("ArrivalTime"))
)

# Convertir la colonne "ArrivalTime" en un type de temps (timestamp)
df = df.withColumn("ArrivalTime", to_timestamp("ArrivalTime", "HH:mm"))

# Formater "ArrivalTime" pour afficher uniquement l'heure au format "HH:mm"
df = df.withColumn("ArrivalTime", date_format("ArrivalTime", "HH:mm"))

# Convertir la colonne "DepartureTime" en un type de temps (timestamp)
df = df.withColumn("DepartureTime", to_timestamp("DepartureTime", "HH:mm"))

# Formater "DepartureTime" pour afficher uniquement l'heure au format "HH:mm"
df = df.withColumn("DepartureTime", date_format("DepartureTime", "HH:mm"))

# Afficher le DataFrame mis à jour
df.show()


# COMMAND ----------

df.dtypes

# COMMAND ----------

from pyspark.sql.functions import year, month, dayofmonth


# Ajouter une colonne "Year" pour l'année
df = df.withColumn("Year", year(df["Date"]))

# Ajouter une colonne "Month" pour le mois
df = df.withColumn("Month", month(df["Date"]))

# Ajouter une colonne "Day" pour le jour
df = df.withColumn("Day", dayofmonth(df["Date"]))

# Afficher le DataFrame mis à jour
display(df)

# COMMAND ----------

df = df.withColumn("Duration", expr(
    "from_unixtime(unix_timestamp(ArrivalTime, 'HH:mm') - unix_timestamp(DepartureTime, 'HH:mm'), 'HH:mm')"
))

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import when, col

# Supposons que votre DataFrame s'appelle "df" et que la colonne "Delay" existe

# Utiliser la fonction "when" pour catégoriser les retards
df = df.withColumn("DelayCategory", 
                   when((col("Delay") >= 1) & (col("Delay") <= 10), "Retard Court")
                   .when((col("Delay") >= 11) & (col("Delay") <= 20), "Retard Moyen")
                   .when(col("Delay") > 20, "Long Retard")
                   .otherwise("Pas de Retard"))  # Par défaut, si pas de retard

# Afficher le DataFrame mis à jour
display(df)


# COMMAND ----------

from pyspark.sql.functions import avg, when, col

# Calculez la moyenne des passagers
avg_passengers = df.select(avg("Passengers")).first()[0]

# Supposons que votre DataFrame s'appelle "df" et que la colonne "Passengers" existe

# Utilisez la fonction "when" pour catégoriser les pointes et hors pointes
df = df.withColumn("Pointe",
                   when(col("Passengers") < avg_passengers, "Hors Pointe")
                   .otherwise("Pointe"))

# Afficher le DataFrame mis à jour
display(df)


# COMMAND ----------

from pyspark.sql.functions import avg, count, format_number


# Calcul du retard moyen par itinéraire
retard_moyen = df.groupBy("Route").agg(avg("Delay").alias("RetardMoyen"))

# Calcul du nombre moyen de passagers par itinéraire
passagers_moyen = df.groupBy("Route").agg(avg("Passengers").alias("PassagersMoyen"))

# Calcul du nombre total de voyages par itinéraire
voyages_total = df.groupBy("Route").agg(count("*").alias("VoyagesTotal"))

# Formater les colonnes numériques avec deux chiffres après la virgule
retard_moyen = retard_moyen.withColumn("RetardMoyen", format_number("RetardMoyen", 2))
passagers_moyen = passagers_moyen.withColumn("PassagersMoyen", format_number("PassagersMoyen", 2))

# Afficher les résultats combinés dans une seule table
result = retard_moyen.join(passagers_moyen, "Route").join(voyages_total, "Route")
result.show()


# COMMAND ----------

df.write \
  .format('csv') \
  .option('header', True) \
  .mode('overwrite') \
  .save("abfss://public-transport-data@fatimazahraestorage.dfs.core.windows.net/processed/df.csv")


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialiser SparkSession
spark = SparkSession.builder.appName("DataTransformation").getOrCreate()

# Répertoire source des fichiers CSV (raw)
source_directory = "abfss://fatimazahraecontainer@fatimazahraestorage.dfs.core.windows.net/raw/"

# Répertoire de destination des fichiers CSV transformés (processed)
destination_directory = "dabfss://fatimazahraecontainer@fatimazahraestorage.dfs.core.windows.net/raw//processed/"

# Liste des fichiers dans le répertoire source
file_list = dbutils.fs.ls(source_directory)

# Parcourir chaque fichier dans le répertoire source
for file_info in file_list:
    file_path = file_info.path
    file_name = file_info.name

    # Charger le fichier CSV
    df = spark.read.csv(file_path, header=True, inferSchema=True)

    # Appliquer les transformations nécessaires sur le DataFrame df
    
    # Par exemple, vous pouvez utiliser df.select(), df.filter(), etc.

    # Sauvegarder le DataFrame transformé dans le répertoire de destination
    df.write.mode("overwrite").csv(destination_directory + file_name)

# Arrêter la session Spark
spark.stop()


# COMMAND ----------


