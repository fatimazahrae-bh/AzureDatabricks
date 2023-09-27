# Databricks notebook source
import pandas as pd
from datetime import datetime
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType,IntegerType
from pyspark.sql.functions import to_date, year, month, day, hour, minute, when, avg, regexp_replace, mean, count, round
from pyspark.sql import SparkSession

from pyspark.sql.functions import when, expr, col
from pyspark.sql.functions import to_timestamp, date_format
from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql.functions import avg, when, col
from pyspark.sql.functions import avg, count, format_number


storageAccountName = "fatimazahraestorage"
storageAccountAccessKey = "hMInODuYZ2hQTHY6mDg5eBMxf9EUFLpWhXtwtHwq88f/te9CBRmr5eeNhyLad6JKmOPGFuv/RqrB+ASt2FlwpQ=="
sasToken = "?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-09-27T16:11:55Z&st=2023-09-27T08:11:55Z&spr=https&sig=a91fRLUOzLoGICh3efWpTBf1XkbLkyOu%2FryHzGq9Q5s%3D"
blobContainerName = "fatimazahraecontainer"
mountPoint = "/mnt/fatimazahraecontainer/"
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
  try:
    dbutils.fs.mount(
      source = "wasbs://{}@{}.blob.core.windows.net".format(blobContainerName, storageAccountName),
      mount_point = mountPoint,
      extra_configs = {'fs.azure.sas.' + blobContainerName + '.' + storageAccountName + '.blob.core.windows.net': sasToken}
    )
    print("mount succeeded!")
  except Exception as e:
    print("mount exception", e)


raw = f"{mountPoint}raw/"
processed = f"{mountPoint}processed/"

processed_count = 0
raw_files = dbutils.fs.ls(raw)
raw_csv_files = [f.path for f in raw_files if f.name.endswith(".csv")]
raw_file_count = len(raw_csv_files)
processed_files = dbutils.fs.ls(processed)
processed_csv_files = [f.path for f in processed_files if f.name.endswith(".csv")]
for i in range(raw_file_count):
    specific_name = "dbfs:" + processed + raw_csv_files[i].replace("raw", "").split("/")[-1].split(".")[0] + "_processed.csv"
    if processed_count == 2:
        break
    elif specific_name in processed_csv_files:
        continue
    else:
        df = spark.read.format("csv").option("inferSchema", "True").option("header", "True").option("delimiter", ",").load(raw_csv_files[i])
        # Vos opérations de transformation ici
        # Puis sauvegardez le DataFrame dans le répertoire "processed"
        


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
# ----------------

        # Ajouter une colonne "Year" 
        df = df.withColumn("Year", year(df["Date"]))

        # Ajouter une colonne "Month" 
        df = df.withColumn("Month", month(df["Date"]))

        # Ajouter une colonne "Day" 
        df = df.withColumn("Day", dayofmonth(df["Date"]))

# ---------------

        # Ajouter la colonne Duration
        df = df.withColumn("Duration", expr(
            "from_unixtime(unix_timestamp(ArrivalTime, 'HH:mm') - unix_timestamp(DepartureTime, 'HH:mm'), 'HH:mm')"
        ))

        #catégoriser les retards
        df = df.withColumn("DelayCategory", 
                        when((col("Delay") >= 1) & (col("Delay") <= 10), "Retard Court")
                        .when((col("Delay") >= 11) & (col("Delay") <= 20), "Retard Moyen")
                        .when(col("Delay") > 20, "Long Retard")
                        .otherwise("Pas de Retard"))  # Par défaut, si pas de retard

        

# --------------------------

        # Calculez la moyenne des passagers
        avg_passengers = df.select(avg("Passengers")).first()[0]

        # catégoriser les pointes et hors pointes
        df = df.withColumn("peakHours",
                        when(col("Passengers") < avg_passengers, "Hors Pointe")
                        .otherwise("Pointe"))

        pandasDF = df.toPandas()
        file_name = f"/dbfs/mnt/fatimazahraecontainer/processed/month" + str(i+1) + ".csv"
        pandasDF.to_csv(file_name, index=False)
        
# -------------------------

        # leretard moyen par itinéraire
        retard_moyen = df.groupBy("Route").agg(avg("Delay").alias("RetardMoyen"))

        # le nombre moyen de passagers par itinéraire
        passagers_moyen = df.groupBy("Route").agg(avg("Passengers").alias("PassagersMoyen"))

        # Le nombre total de voyages par itinéraire
        voyages_total = df.groupBy("Route").agg(count("*").alias("VoyagesTotal"))

        # Formater les colonnes numériques avec deux chiffres après la virgule
        retard_moyen = retard_moyen.withColumn("RetardMoyen", format_number("RetardMoyen", 2))
        passagers_moyen = passagers_moyen.withColumn("PassagersMoyen", format_number("PassagersMoyen", 2))

        # Afficher les résultats combinés dans une seule table
        result = retard_moyen.join(passagers_moyen, "Route").join(voyages_total, "Route")

        # Sauvegarde des résultats de groupe (par itinéraire)
        result_pandas = result.toPandas()
        result_filename = "/dbfs/mnt/fatimazahraecontainer/processed/month" + str(i+1) + ".csv"
        result_pandas.to_csv(result_filename, index=False)

    
# -----------------------------
    processed_count += 1

dbutils.fs.unmount("/mnt/fatimazahraecontainer/")
