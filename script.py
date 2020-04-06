import argparse

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions as F

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('--filepath', type=str, required=True,
                    help='O caminho dos logs no hdfs')

args = parser.parse_args()

filepath = args.filepath

# Nome das colunas usadas no DataFrame
columns = [
    "host",
    "timestamp",
    "request",
    "http_code",
    "total_bytes"
]

regex = '(\S+) - - (\[.*\]) "(.*)" (\d{3}) (\S+)'
date_regex = '\[(\d{2}\/\S{3}\/\d{4})'
url_regex = '(\S+) HTTP'

sc = SparkContext("local", "Semantix Nasa Test")
sql = SQLContext(sc)

print("Carregando dataset...")
logs = sql.read.option("header", "false").csv(filepath)

# Quebra as linhas do arquivo de log usando uma expressao regular
for k, column in enumerate(columns):
    logs = logs.withColumn(column, F.regexp_extract("_c0", regex, k + 1))

def count_unique_hosts():
    result = logs.groupby("host").count()
    count = result.count()
    
    print("Existem {} hosts unicos no dataset".format(count))
    
def count_404_results():
    result = logs.filter("http_code like '%404%'")

    count = result.count()
    
    print("Existem {} urls com codigo de retorno HTTP 404 no dataset".format(count))

def which_urls_caused_more_404():
    result = logs.filter("http_code like '%404%'")
    result = result.withColumn("url", F.regexp_extract("request", url_regex, 1))
    result = result.groupby(["url", "http_code"]).count()
    result = result.sort(F.desc("count"))

    rows = result.take(5)

    for row in rows:
        print("URL: {} - ERROS: {}".format(row['url'], row['count']))

def counting_of_404_by_day():
    result = logs.filter("http_code like '%404%'")
    result = result.withColumn("date", F.regexp_extract("timestamp", date_regex, 1))
    result = result.groupby(["date", "http_code"]).count()
    result = result.sort(F.asc("date"))
    
    for row in result.collect():
        print("DATA: {} - ERROS: {}".format(row["date"], row["count"]))

def calc_total_bytes():
    result = logs.withColumn("total_bytes", logs.total_bytes.cast("int"))
    result = result.groupby().sum()
    
    count = result.take(1)[0]["sum(total_bytes)"]
    
    print("O total de bytes retornados foi: {}b".format(count))

def show_results():
    print("===============================================")
    print("DESAFIO SEMANTIX")
    print("RESPOSTAS:")
    print("===============================================")

    print("===============================================")
    print("Questao 1: Numero de hosts unicos.\n")

    count_unique_hosts()

    print("===============================================")
    print("Questao 2: O total de erros 404.\n")

    count_404_results()

    print("===============================================")
    print("Questao 3: Os 5 URLs que mais causaram erro 404.\n")

    which_urls_caused_more_404()

    print("===============================================")
    print("Questao 4: Quantidade de erros 404 por dia.\n")

    counting_of_404_by_day()

    print("===============================================")
    print("Questao 5: O total de bytes retornados.\n")

    calc_total_bytes()

    print("===============================================")
    print("FIM")

if __name__ == '__main__':
    show_results()
