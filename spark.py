from pyspark.sql import SparkSession

# Inisialisasi SparkSession dengan akses ke Hadoop HDFS
spark = SparkSession.builder \
    .appName("Read CSV Column from Hadoop") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop:8020") \
    .getOrCreate()

# Lokasi file CSV di HDFS
csv_path = "hdfs://hadoop:8020/sheet/Tweet.csv"

# Membaca file CSV dari HDFS
df = spark.read.csv(csv_path, header=True, inferSchema=True)

print(df)
# Memilih kolom yang diinginkan. Misalnya, kita ingin membaca kolom 'name' dan 'age'
selected_columns = df.select("name", "age")

# Menampilkan data dari kolom yang dipilih
selected_columns.show()

# Jangan lupa untuk menghentikan SparkSession setelah selesai
spark.stop()
