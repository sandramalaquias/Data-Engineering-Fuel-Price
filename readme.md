![](https://github.com/sandramalaquias/Data-Engineering-Fuel-Price/blob/master/fuel.png)

# Project: Fuel price analysis 

Brazil closed the first half of 2022 with a 5.49% rise in official consumer price inflation, according to the IBGE (Brazilian Institute of Geography and Statistics). The price of a liter of gasoline rose 9.8% in the period, according to the ANP (National Agency for Petroleum, Natural Gas and Biofuels). In other words, the fuel soared more than the official inflation.


The question here is: how does one see the timeline of fuel prices in Brazil, its states, and cities? It's possible to compare with other indicators, such as the dollar price, the oil barrel price, the minimum wage, and inflation?

To answer these questions, we can use the open databases provided by Brazilian agencies, through APIs and URIs.
 
The information that will be extracted and stored as tables will be: 
- dollar price timeline 
- oil barrel price timeline 
- Brazilian inflation timeline
- minimum wage timeline 
- data from Brazilian cities
- fuel price timeline

Each table has a different source. Check "tables.ods" for more information.

This project consists of several stages: 
- extracting data from the source using AIRFLOW 
- data transformation using SPARK in docker 
- loading data into Redshift using AIRFLOW again 
- selecting and downloading data for analysis using the Redshift editor 
- create simple graphics using a python script.

# Data Modeling
[See source data](https://dbdiagram.io/embed/634ea66d4709410195888817)
![](https://github.com/sandramalaquias/Data-Engineering-Fuel-Price/blob/master/fuel_initial.png)

[See the connect fields](https://dbdiagram.io/embed/633446987b3d2034ffd9aa36)
![](https://github.com/sandramalaquias/Data-Engineering-Fuel-Price/blob/master/fuel_initial_keys.png)

[See the Data Purpose](https://dbdiagram.io/embed/634ebb2c470941019589cbee)
![](https://github.com/sandramalaquias/Data-Engineering-Fuel-Price/blob/master/fuel_final.png)

# ELT / ETL - Airflow

In this step, we will extract the data from the source and store it in S3, in raw form, or with some transformation.
I used Docker <a href="https://airflow.apache.org/docs/docker-stack/">Apache Airflow - Docker - version [v2.3.3]</a>
In Airflow, register your AWS credentials in connection Choose -> Admin > Connection -> + (add a new record) with:
```
 - Connection Id: "credentials"
 - Connection Type: choose "Amazon Web Service"
 - Description: "credentials to aws"
 - Login: your AWS access key
 - Password: your AWS secret key 
```

## Extract and load (ELT)

These tables will be affected by this process:
   - dollar: exchange rate
   - wage: minimum wage 
   - city: data of cities
   - IPCA: inflation index
   - Brent: the price of Brent oil
   
To obtain this data, run the "**file_to_s3**" dag found in the "files_to_s3.py" script. 

This dag was scheduled to run monthly, due to the data period that the source is updated. It will always take all available data and update the S3.

## Extract, transform, and load (ETL)

The fuel table contains all fuel prices for each gas station in all Brazilian cities and has a lot of rows, more than 1 million.
 
To get this information, I could:
- extract it, store this data in raw form, and later make the transformations using spark. In this case, an EMR cluster would be needed (it has an additional cost).

Instead what I choose to do was:
- extract it, calculate the average by city, and load it in S3. How the extraction is per file that contains an average of 400,000 lines, doing this transformation via Airflow would be more affordable. 

Assuming that the gas station data are not relevant for the analysis, and in the end, we will only work with average monthly prices in each city, this was the transformation carried out via Airflow.

It will be necessary to run two dags because the historical and current data are not aggregated (one is semiannual,  and the other is monthly).

- Dag **"fuel_hist"** found at "new_fuel.py". The schedule is per semester, starting in 2007 and ending in 2021. This dag can be turned off after picking all historical info.

- Dag **"fuel_current_year"** found at "new_fuel_2022.py". The schedule is per month, starting in January 2022, and ending in August 2022. At the end of all periods, if it's necessary to keep the monthly extraction, remove the end date from the script and keep it active, or turn off the dag if that isn't necessary.

# ETL - Spark / Pyspark


The script in Pyspark was used to:
- harmonize the dates in all tables
- calculate the average when necessary
- create the state table from the city
- create the relationship indexes 
- create manifest files to upload to Redshift. 

Don't forget to update the file "aws.cfg" before running this script.

Run the **"dw_fuel.py"** via spark-submit, like:

    spark-submit dw_fuel.py --inputpath s3a://smmbucket/myproject/ --outputpath s3a://smmbucket/myproject/output/ --configpath /usr/spark-2.4.1/data/aws.cfg --localrun y --fileout json --writeout y

Configurations:
- inputpath: bucket input path
- outputpath: bucket output path
- configpath: AWS credential file path 
- localrun: "Y" - to run via docker or "N".
- fileout: output file type like "JSON" or "PARQUET"
- writeout: 'y' - to write to S3 or 'n' for testing

In the end, the data was configured to Redshift and could be uploaded via "manifest file".

# DW - Redshift
To enable analysis from data at the S3 stage, I have these options:  
- use the Glue and Athena and gather the data
- or push the data to Redshift, that was my choice.

Create the cluster:
- update credential into "dwh1.cfg". 
- run "AWS_redshift_create_cluster.py". Take note of the endpoint showned, which will be used to update Airflow.

- Update o Airflow at Admin > Connection -> + (add a new record):
```
  - Connection Id: redshift   
  - Connection Type: choose "Postgres"	
  - Description: connect Airflow to Redshift using Postgres
  - Host: your redshift endpoint
  - Schema: your schema name  
  - Login: your username  
  - Password: your user redshift password 
  - Port: 5439
```

At Airflow, run dag "**FuelToRedshift**" that is in "fuel_to_redshift.py". This Dag doesn't have an automatic start, then starts manually.
You can run it whenever you find it necessary, in the end, the Redshift will be up to date.

# Redshift
Directly in Redshift, execute the selection below to extract the data for analysis.

    select *
    from fuel.fuel  as fuel
    join fuel.usd   as usd   on fuel.fuel_year_month = usd.usd_year_month
    join fuel.ipca  as ipca  on fuel.fuel_year_month = ipca.ipca_year_month
    join fuel.brent as brent on fuel.fuel_year_month = brent.brent_year_month
    join fuel.wage  as wage  on fuel.fuel_year_month = wage.wage_year_month
    where fuel.fuel_product_name = 'GASOLINA' and
          fuel.fuel_city_code = 355030
          
Download the result in the "csv" file, which can be found at "fuel_result.csv".

# Data Analysis

The script "gas.py" was used to get some insights from the data.  

Run that script like this:

    python3 gas.py --inputpath /home/sandra/Downloads/fuel_result.csv
Configuration:
- inputpath: the file path to use, like ".../fuel_result.csv"

In the end, take the files below:
- graph_fuel_timeline.jpg - timeline graph to see the evolution of all data 
- fuel_gas.html - statistic analysis (pandas profiling)

From the graphs, we can see that the best gasoline purchasing power for São Paulo residents was during the Dilma/Temer government, between 2013 and 2017.
In August 2022, São Paulo residents have gasoline purchasing power at the levels of 2007, which is the initial year of the analysis.

# Folders in this project

- DAGS: contains all the scripts to Airflow. This folder includes dags, operators, and helpers
- PYSPARK: contains all the pyspark scripts 
- Scripts: contains all the python scripts (like creating an AWS redshift cluster or creating a data analysis)
- Notebook: notebook used to support 
- Data - support files. The files "aws.cfg" and "dwh1.cfg" must be updated with AWS credentials.

