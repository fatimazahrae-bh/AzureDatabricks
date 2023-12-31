# Databricks notebook source
# Mounting data lake
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

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import pandas as pd
import random
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

# COMMAND ----------

for month in range(1, 6):
    # Generate data for the current month
    start_date = datetime(2023, month, 1)
    end_date = start_date.replace(day=1, month=month+1) - timedelta(days=1)
    date_generated = [start_date + timedelta(days=x) for x in range(0, (end_date - start_date).days + 1)]

    transport_types = ["Bus", "Train", "Tram", "Metro"]
    routes = ["Route_" + str(i) for i in range(1, 11)]
    stations = ["Station_" + str(i) for i in range(1, 21)]

    # Randomly select 5 days as extreme weather days
    extreme_weather_days = random.sample(date_generated, 5)

    data = []

    for date in date_generated:
        for _ in range(32):  # 32 records per day to get a total of 992 records for January
            transport = random.choice(transport_types)
            route = random.choice(routes)

            # Normal operating hours
            departure_hour = random.randint(5, 22)
            departure_minute = random.randint(0, 59)

            # Introducing Unusual Operating Hours for buses
            if transport == "Bus" and random.random() < 0.05:  # 5% chance
                departure_hour = 3

            departure_time = f"{departure_hour:02}:{departure_minute:02}"

            # Normal duration
            duration = random.randint(10, 120)

            # Introducing Short Turnarounds
            if random.random() < 0.05:  # 5% chance
                duration = random.randint(1, 5)

            # General delay
            delay = random.randint(0, 15)

            # Weather Impact
            if date in extreme_weather_days:
                # Increase delay by 10 to 60 minutes
                delay += random.randint(10, 60)

                # 10% chance to change the route
                if random.random() < 0.10:
                    route = random.choice(routes)

            total_minutes = departure_minute + duration + delay
            arrival_hour = departure_hour + total_minutes // 60
            arrival_minute = total_minutes % 60
            arrival_time = f"{arrival_hour:02}:{arrival_minute:02}"

            passengers = random.randint(1, 100)
            departure_station = random.choice(stations)
            arrival_station = random.choice(stations)

            data.append([date, transport, route, departure_time, arrival_time, passengers, departure_station, arrival_station, delay])
    
    # Create a DataFrame for the current month's data
    month_i = str(month).zfill(2) + "_" + str(start_date.year)
    df = pd.DataFrame(data, columns=["Date", "TransportType", "Route", "DepartureTime", "ArrivalTime", "Passengers", "DepartureStation", "ArrivalStation", "Delay"])

    # Save the DataFrame to a CSV file with the proper name
    file_name = f"/dbfs/mnt/fatimazahraecontainer/raw/DataOf_{month_i}.csv"
    df.to_csv(file_name, index=False)

    print(f"Generated data for month {month_i} and saved to {file_name}")
   #----------------- 

# COMMAND ----------

dbutils.fs.unmount("/mnt/fatimazahraecontainer/")
