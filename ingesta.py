import mysql.connector
import csv
import boto3
import io

db_connection = mysql.connector.connect(
    host="3.217.247.83",
    user="root",
    password="utec",
    database="tienda"
)

cursor = db_connection.cursor()

cursor.execute("SELECT * FROM fabricantes")

rows = cursor.fetchall()

column_names = [i[0] for i in cursor.description]

csv_buffer = io.StringIO()

csv_writer = csv.writer(csv_buffer)
csv_writer.writerow(column_names)
csv_writer.writerows(rows)

cursor.close()
db_connection.close()

s3 = boto3.client('s3')
nombreBucket = "antonio-ingesta02-s3"
nombreArchivoS3 = "data.csv"

s3.put_object(Bucket=nombreBucket, Key=nombreArchivoS3, Body=csv_buffer.getvalue())

print("Ingesta completada")
