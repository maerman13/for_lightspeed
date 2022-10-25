from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import *

def second(input_year, input_month, query):
    spark = SparkSession.builder.config("spark.jars", "postgresql-42.2.14.jar").master("local[*]").getOrCreate()
    spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/andreimaiorov") \
        .option("dbtable", "(" + query + ") as q") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .load() \
        .select(col("ID"), col("status"), to_date(to_timestamp(from_unixtime('date'))).alias("date"), year(to_timestamp(from_unixtime('date'))).alias("year"), month(to_timestamp(from_unixtime('date'))).alias("month"), col("time")) \
        .where((col("year") == input_year) & ((col("month") == input_month))) \
        .withColumn("row_num", row_number().over(Window.partitionBy("ID", "date").orderBy(col("time").cast('integer').desc()))) \
        .where(col("row_num") == 1) \
        .select("id", "date", "status") \
        .write \
        .partitionBy("ID") \
        .csv("/Users/andreimaiorov/Desktop/test_result_1")

def fourth(path):
    spark = SparkSession.builder.config("spark.jars", "postgresql-42.2.14.jar").master("local[*]").getOrCreate()
    id = spark.read.csv(path).select("id").head()[0]
    query = f""" 
        select
        date_from,
        date_end,
        id,
        status
        from
        (
        select
        to_timestamp(CAST(date as bigint)) as date,
        LAG(to_timestamp(CAST(date as bigint))) OVER(PARTITION BY id ORDER BY date) date_from, 
        LEAD(to_timestamp(CAST(date as bigint))) OVER(PARTITION BY id ORDER BY date) date_end,
        LAG(status) OVER(PARTITION BY id ORDER BY date) prev_code, 
        LEAD(status) OVER(PARTITION BY id ORDER BY date) next_code,
        id,
        status
        from
        public.cyclones
        where id='{id}'
        ) tmp
        where next_code != prev_code
    """
    spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/andreimaiorov") \
        .option("dbtable", "(" + query + ") as q") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .load() \
        .write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/andreimaiorov") \
        .option("dbtable", "public.cyclones_history") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .save()

def fifth(path):
    spark = SparkSession.builder.config("spark.jars", "postgresql-42.2.14.jar").master("local[*]").getOrCreate()
    df = spark.read.csv(path).show()
    df.show()

if __name__ == '__main__':
    # 2. Написать ETL-скрипт, который для указанного календарного месяца генерирует CSV-файлы с данными из cyclones.
    # Данные для каждого дня месяца должны быть в отдельном файле. Имена файлов сделать вида cyclones_20140128.csv
    second(1970, 8, "select * from public.cyclones")

    # 3. Сгенерировать при помощи полученного в п.2 скрипта файлы для date >= 2013-01-01
    second(1970, 8, "select  * from public.cyclones where to_timestamp(CAST(date as bigint)	) >= DATE '2013-01-01'")

    # 4. Написать второй ETL-скрипт, который будет уметь принимать один файл вида cyclones_20140128.csv и формировать историю статусов циклонов в таблице cyclones_history в PostgreSQL.
    fourth("/Users/andreimaiorov/Desktop/test_result_1/cyclones_19700803.csv")

    # 5. Загрузить историю из файлов полученных в п.3
    fifth("/Users/andreimaiorov/Desktop/test_result_1/")
