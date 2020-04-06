# Teste para Engenheiro de Dados da Semantix

## HTTP​ ​requests​ ​to​ ​the​ ​NASA​ ​Kennedy​ ​Space​ ​Center​ ​WWW​ ​server

### 1. Respostas

Seguem abaixo minhas respostas para as questões levantadas no desafio. Atente-se, as respostas são fruto de minha experiência e 
percepções a respeito da tecnologia, portanto não devem ser tomadas como verdades. Para isso, seria necessário mais pesquisa
e análise do código fonte do Spark.

#### Qual o objetivo do comando cache​ ​em Spark?

Como o nome sugere, o comando cache é usado para persistir objetos - como DataFrames, SQL Queries, RDD, TempTables - na memória 
permitindo um acesso mais rápido do que o reprocessamento tipico realizado pela lazy evaluation do Spark. Por exemplo, vamos 
supor que você precisa realizar uma comparação de-para entre as linhas de diferentes dataframes com um único dataframe que contém 
essa relação. Nesse caso vale a pena realizar o cache de forma que os nodes do cluster tenham uma representação na memória ao
ao invés de processá-lo repetidamente.

#### O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em MapReduce. Por quê?

Além das inúmeras otimizações que o Spark faz a DAG criada por seu script, o Spark também utliza-se de uma técnica diferente de
processamento distribuido do MapReduce, que, ao invés de quebrar o processamento em inúmeras etapas de Map e Reduce, o spark cria
uma complexa representação do processamento que pode ser executado em ordens diferentes e de maneira lazy, dependendo apenas das 
ações tomadas no código. Por exemplo, uma simples filtragem e contagem - no Spark seria equivalente ao groupBy(key).count() - além 
de mais complexo de implementar no Map Reduce, requer que cada etapa de Map seja carregada na memória e executada para a seguinte de 
Reduce seja também, só então a memria e processamento serão liberados. Já no Spark o processo é diferente, devido os RDD - blocos
de memória que carregam lazymente dados para tratamento -, há uma inteligência no cluster capaz de entender qual a melhor sequência
de operações para realizar a task da forma mais eficiente possivel.

#### Qual é a função do SparkContext​?

O Spark Context encapsula as principais funçes que se comunicam com cluster e hdfs. Nele você define o host e as configuracoes
para o processamento da sua task.

#### Explique com suas palavras o que é Resilient​ ​Distributed​ ​Datasets​ (RDD).

O RDD é o que faz do Spark uma plataforma de computação distribuída, pois ele permite que
pacote de dados sejam tratados em paralelo e com consistência garantida, além de fazer
uso da própria forma como os dados são armazenados no HDFS - em blocos. Também, os RDD 
expõem diversas funçoes que sao executadas de forma lazy, ou seja, apenas quando o programa 
encontra uma ação - tal como show, collect, take, etc -. Ademais, o DataFrame é uma 
abstração acima do RDD, que permite o tratamento de tabelas, semelhante ao SQL.

#### GroupByKey​ ​é menos eficiente que reduceByKey​ ​em grandes dataset. Por quê?

Pois para realizar o groupByKey você precisa conferir o estado de todos os nós 
rodando a operação, enquanto que o reduceByKey, como o nome sugere, é uma operação 
de redução sequencial que pode ser executada paralela e depois somados os resultados.

#### Explique o que o código abaixo faz:

```scala
val textFile = sc.textFile("hdfs://...")
val counts = textFile.flatMap(line => line.split(" "))
.map(word => (word, 1))
.reduceByKey(_ + _)
counts.saveAsTextFile("hdfs://...")
```

Primeiramente, lê-se um arquivo qualquer do hdfs a partir de uma instância do Spark Context. Entretando, é importante
ressaltar que o arquivo não foi lido ainda, apenas checa-se sua existência e integridade. Após isso, realiza-se um
mapeamento em todas linhas do arquivo - no caso apenas uma - quebrando-as por espaço em branco, o que resulta em um RDD com
um array de strings. Em seguida, outro mapeamento é realizado onde para cada item do array é definido o número 1. Por fim, 
é realizado um reduceByKey, que na linguagem Scala pode ser simplificado com essa notação simbólica, pois, para cada palavra
há um número 1 e ao somar todos você obtém a quantidade de cada palavra no texto.

Então, após a construção da DAG deste programa, temos a ação de salvar o resultado no HDFS, essa sim irá iniciar o processamento
no cluster e somente quanto todos os nós terminarem que o arquivo estará disponível e o script encerrará.


### 2. Solução

O script consiste numa implementação simples usando o Python, portanto PySpark, que faz os devidos processamentos esperados 
para cada questão levantada no desafio.

Para executá-lo basta:

```
python script.py --filepath=/hdfs/caminho/arquivo/logs
```

