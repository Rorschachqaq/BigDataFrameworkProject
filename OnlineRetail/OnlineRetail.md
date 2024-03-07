# 零售购物记录关联分析
* Eidted By **Rorschach**

## 一、下载数据并解压
```
unzip online+retail.zip
```
<br>

## 二、准备阶段
### 首先准备Spark环境
```
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("SparkDemo").master("local").getOrCreate()
```
<br>

### 借助pandas读取数据
```
import pandas as pd
from mlxtend.frequent_patterns import apriori
from mlxtend.frequent_patterns import association_rules
```
<br>


### 查看jupyter文件存取路径
```
import os
print(os.getcwd())
```
`/home/hadoop/jupyternotebook/OnlineRetail`
<br>


### 读取.xlsx文件，写入.csv文件
```
pandas_df = pd.read_excel('/home/hadoop/jupyternotebook/OnlineRetail/Online_Retail.xlsx')
pandas_df.to_csv('/home/hadoop/jupyternotebook/OnlineRetail/Online_Retail.csv', index=False)
```
<br>


#### 接下来需要将文件上传至HDFS中
首先打开hdfs
```
hadoop@rorschach-virtual-machine:~/Desktop$ cd /usr/local/hadoop
hadoop@rorschach-virtual-machine:/usr/local/hadoop$ ./sbin/start-dfs.sh
```
打开成功
```
Starting namenodes on [localhost]
localhost: namenode is running as process 34576.  Stop it first and ensure /tmp/hadoop-hadoop-namenode.pid file is empty before retry.
Starting datanodes
localhost: datanode is running as process 34717.  Stop it first and ensure /tmp/hadoop-hadoop-datanode.pid file is empty before retry.
Starting secondary namenodes [rorschach-virtual-machine]
rorschach-virtual-machine: secondarynamenode is running as process 34963.  Stop it first and ensure /tmp/hadoop-hadoop-secondarynamenode.pid file is empty before retry.
```
上传文件并查看文件是否上传成功
```
hadoop@rorschach-virtual-machine:/usr/local/hadoop$ ./bin/hdfs dfs -put /home/hadoop/jupyternotebook/OnlineRetail/Online_Retail.csv /sparksql/input
hadoop@rorschach-virtual-machine:/usr/local/hadoop$ ./bin/hdfs dfs -ls /sparksql/input
```
上传成功
```
Found 3 items
-rw-r--r--   1 hadoop supergroup   48039726 2023-10-27 11:37 /sparksql/input/Online_Retail.csv
-rw-r--r--   1 hadoop supergroup    1493648 2023-10-13 13:37 /sparksql/input/movie.csv
-rw-r--r--   1 hadoop supergroup  690353377 2023-10-13 13:38 /sparksql/input/rating.csv
```
<br>


### 将.csv文件数据读取到Spark Dateframe中
```
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.read.csv('hdfs://localhost:9000/sparksql/input/Online_Retail.csv', header=True,inferSchema=True)
df.show(10)
```
```
+---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
|InvoiceNo|StockCode|         Description|Quantity|        InvoiceDate|UnitPrice|CustomerID|       Country|
+---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
|   536365|   85123A|WHITE HANGING HEA...|       6|2010-12-01 08:26:00|     2.55|   17850.0|United Kingdom|
|   536365|    71053| WHITE METAL LANTERN|       6|2010-12-01 08:26:00|     3.39|   17850.0|United Kingdom|
|   536365|   84406B|CREAM CUPID HEART...|       8|2010-12-01 08:26:00|     2.75|   17850.0|United Kingdom|
|   536365|   84029G|KNITTED UNION FLA...|       6|2010-12-01 08:26:00|     3.39|   17850.0|United Kingdom|
|   536365|   84029E|RED WOOLLY HOTTIE...|       6|2010-12-01 08:26:00|     3.39|   17850.0|United Kingdom|
|   536365|    22752|SET 7 BABUSHKA NE...|       2|2010-12-01 08:26:00|     7.65|   17850.0|United Kingdom|
|   536365|    21730|GLASS STAR FROSTE...|       6|2010-12-01 08:26:00|     4.25|   17850.0|United Kingdom|
|   536366|    22633|HAND WARMER UNION...|       6|2010-12-01 08:28:00|     1.85|   17850.0|United Kingdom|
|   536366|    22632|HAND WARMER RED P...|       6|2010-12-01 08:28:00|     1.85|   17850.0|United Kingdom|
|   536367|    84879|ASSORTED COLOUR B...|      32|2010-12-01 08:34:00|     1.69|   13047.0|United Kingdom|
+---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
only showing top 10 rows
```
<br>


#### 其中数据字典如下

|InvoiceNo|StockCode|Description|Quantity|InvoiceDate|UnitPrice|    CustomerID|Country|
|-|-|-|-|-|-|-|-|
|发票编号|商品ID|名称|数量|日期|总价|顾客ID|国家|

其中InvoiceNo相同代表是同一次交易，也是我们进行关联分析的关键
接下来进行数据预处理
处理为每一行代表一次交易

```
data = df.select('InvoiceNo', 'StockCode').rdd.reduceByKey(lambda a, b: a + ',' + b)
data.take(10)
```
```
[('536365', '85123A,71053,84406B,84029G,84029E,22752,21730'),
 ('536366', '22633,22632'),
 ('536367',
  '84879,22745,22748,22749,22310,84969,22623,22622,21754,21755,21777,48187'),
 ('536368', '22960,22913,22912,22914'),
 ('536369', '21756'),
 ('536370',
  '22728,22727,22726,21724,21883,10002,21791,21035,22326,22629,22659,22631,22661,21731,22900,21913,22540,22544,22492,POST'),
 ('536371', '22086'),
 ('536372', '22632,22633'),
 ('536373',
  '85123A,71053,84406B,20679,37370,21871,21071,21068,82483,82486,82482,82494L,84029G,84029E,22752,21730'),
 ('536374', '21258')]
```
<br>


#### 此时data中存储了以交易id为键、商品id串为值的记录，将商品id串转换为数组
```
data = data.map(lambda x: x[1].split(','))
data.take(10)
```

```
[['85123A', '71053', '84406B', '84029G', '84029E', '22752', '21730'],
 ['22633', '22632'],
 ['84879','22745','22748','22749','22310','84969','22623','22622'，'21754'，'21755','21777','48187'],
 ['22960', '22913', '22912', '22914'],
 ['21756'],
 ['22728','22727','22726','21724','21883','10002','21791','21035','22326','22629','22659','22631','22661','21731','22900','21913','22540','22544','22492','POST'],
 ['22086'],
 ['22632', '22633'],
 ['85123A','71053','84406B','20679','37370','21871','21071','21068','82483','82486','82482','82494L','84029G','84029E','22752','21730'],
 ['21258']]
```
<br>



由于上面的数据中某些交易记录存在重复商品，因此需要对每个数组进行去重操作，使用**python** 中的 **set** 来完成
```
unique_data = data.map(lambda x: list(set(x)))
unique_data.count()
```
```
25900
```
<br>



## 三、关联规则的生成
### 调用 **mllib** 中的 **FPGrowth** 算法计算频繁项集
```
from pyspark.mllib.fpm import FPGrowth
model = FPGrowth.train(unique_data, 0.03)
fre_item = sorted(model.freqItemsets().collect())
``` 
<br>


### 借助spark.ml库中的FPGrowth生成关联规则
将rdd处理为DataFrame，并将rdd存入csv文件
```
t = unique_data.map(lambda x: ' '.join(x))
t.saveAsTextFile('unique_data.csv')
```
<br>


### 读取到DataFrame
```
from pyspark.sql.functions import split
data = (spark.read.csv("unique_data.csv")).select(split("_c0", " ").alias("items"))
```
<br>

### 训练FPGrowth模型，输出关联规则
```
from pyspark.ml.fpm import FPGrowth
fp = FPGrowth(minSupport=0.01, minConfidence=0.5)
fpm = fp.fit(data)
fpm.freqItemsets.show()
fpm.associationRules.show()
```
#### 频繁项集
```
+--------------------+----+
|               items|freq|
+--------------------+----+
|             [21889]| 590|
|             [21981]| 272|
|             [22776]| 576|
|             [22819]| 270|
|             [22501]| 472|
|             [21257]| 268|
|             [35970]| 463|
|               [DOT]| 710|
|        [DOT, 20712]| 361|
| [DOT, 20712, 21931]| 260|
|[DOT, 20712, 85099B]| 273|
|        [DOT, 22697]| 331|
|        [DOT, 21731]| 313|
|        [DOT, 21931]| 395|
|[DOT, 21931, 85099B]| 288|
|        [DOT, 22457]| 290|
|       [DOT, 85099B]| 485|
|        [DOT, 22386]| 345|
|[DOT, 22386, 85099B]| 277|
|        [DOT, 22379]| 338|
+--------------------+----+
only showing top 20 rows
```
<br>


#### 关联规则
```
+---------------+----------+------------------+------------------+--------------------+
|     antecedent|consequent|        confidence|              lift|             support|
+---------------+----------+------------------+------------------+--------------------+
| [22384, 22382]|   [20728]| 0.617169373549884|  13.6271839513572| 0.01027027027027027|
| [22384, 22382]|   [20725]| 0.679814385150812|10.949746626496289|0.011312741312741313|
| [22384, 22382]|   [20727]| 0.617169373549884| 12.34338747099768| 0.01027027027027027|
|        [21094]|   [21080]|0.7020872865275142|18.404919758160542|0.014285714285714285|
|        [21094]|   [21086]|0.6641366223908919| 40.18957598113108|0.013513513513513514|
| [20723, 20724]|   [22355]|0.6857142857142857|19.843575418994416|0.012972972972972972|
| [20723, 20724]|   [20719]|0.6693877551020408|  20.5660057617353|0.012664092664092665|
| [20723, 20724]|   [22356]|0.6693877551020408| 22.81203007518797|0.012664092664092665|
|   [DOT, 20712]|   [21931]|0.7202216066481995|15.531839810315043|0.010038610038610039|
|   [DOT, 20712]|  [85099B]|0.7562326869806094| 9.173970301076245| 0.01054054054054054|
|        [21231]|   [21232]|0.6473880597014925| 18.88215174129353|0.013397683397683397|
|        [22417]|   [21212]|0.5766283524904214|11.195408043104884|0.011621621621621621|
|        [82581]|   [82580]|0.7170731707317073|29.201564657156005|0.011351351351351352|
|        [22866]|   [22865]| 0.600358422939068| 23.31226859688435|0.012934362934362934|
|        [22866]|   [22867]|0.5125448028673835| 23.62083700047194|0.011042471042471042|
|[22411, 85099B]|   [21931]|0.5724743777452416|12.345617305247092|0.015096525096525096|
|[22411, 85099B]|   [22386]|0.5563689604685212|11.705894456648823|0.014671814671814672|
|   [DOT, 22411]|  [85099B]|0.7713498622589532| 9.357358984780744|0.010810810810810811|
|        [21914]|   [21915]|0.5223140495867769| 20.16085526720942|0.012200772200772201|
| [20728, 20725]|   [20726]|  0.50355871886121|12.576828175993574|0.010926640926640927|
+---------------+----------+------------------+------------------+--------------------+
only showing top 20 rows
```
<br>


#### 将结果按照提升度排序
```
from pyspark.sql.functions import desc
rules = fpm.associationRules.orderBy(desc('lift'))
```
<br>


#### 将商品id与名称想对应
构建商品id到名称的字典（注意取出原始数据中的空值，删除原始数据名称为空的交易记录）
```
df2 = df.dropna().select('StockCode', 'Description').distinct()
code_items = {}
for r in df2.collect():
    code_items[r['StockCode']] = r['Description']
```
<br>


#### 将关联规则中的字段进行替换
```
from pyspark.sql.functions import udf
item_udf = udf(lambda x: [code_items[code] for code in x])

rules_with_name = rules.withColumn('antecedent', item_udf('antecedent')).withColumn('consequent', item_udf('consequent'))
```
<br>


### 得到关联规则
```
rules_with_name.show(20, False)
```

```
+--------------------------------------------------------------------+------------------------------------+------------------+------------------+--------------------+
|antecedent                                                          |consequent                          |confidence        |lift              |support             |
+--------------------------------------------------------------------+------------------------------------+------------------+------------------+--------------------+
|[REGENCY TEA PLATE GREEN ]                                          |[REGENCY TEA PLATE PINK]            |0.7305699481865285|60.26038744595888 |0.010888030888030888|
|[REGENCY TEA PLATE PINK]                                            |[REGENCY TEA PLATE GREEN ]          |0.8980891719745223|60.26038744595888 |0.010888030888030888|
|[REGENCY TEA PLATE PINK]                                            |[REGENCY TEA PLATE ROSES ]          |0.8662420382165605|49.093367154942925|0.010501930501930502|
|[REGENCY TEA PLATE ROSES ]                                          |[REGENCY TEA PLATE PINK]            |0.5951859956236324|49.093367154942925|0.010501930501930502|
|[POPPY'S PLAYHOUSE BEDROOM ]                                        |[POPPY'S PLAYHOUSE LIVINGROOM ]     |0.6150234741784038|48.270024185517144|0.010115830115830116|
|[POPPY'S PLAYHOUSE LIVINGROOM ]                                     |[POPPY'S PLAYHOUSE BEDROOM ]        |0.793939393939394 |48.270024185517144|0.010115830115830116|
|[REGENCY TEA PLATE ROSES ]                                          |[REGENCY TEA PLATE GREEN ]          |0.7045951859956237|47.27724175462863 |0.012432432432432432|
|[REGENCY TEA PLATE GREEN ]                                          |[REGENCY TEA PLATE ROSES ]          |0.8341968911917098|47.277241754628626|0.012432432432432432|
|[POPPY'S PLAYHOUSE KITCHEN]                                         |[POPPY'S PLAYHOUSE LIVINGROOM ]     |0.5977272727272728|46.91253443526171 |0.010154440154440154|
|[POPPY'S PLAYHOUSE LIVINGROOM ]                                     |[POPPY'S PLAYHOUSE KITCHEN]         |0.796969696969697 |46.912534435261705|0.010154440154440154|
|[SET/6 RED SPOTTY PAPER CUPS, SET/20 RED RETROSPOT PAPER NAPKINS ]  |[SET/6 RED SPOTTY PAPER PLATES]     |0.8952702702702703|43.999051233396585|0.010231660231660231|
|[PINK HAPPY BIRTHDAY BUNTING]                                       |[BLUE HAPPY BIRTHDAY BUNTING]       |0.6633165829145728|43.71475699106218 |0.010193050193050193|
|[BLUE HAPPY BIRTHDAY BUNTING]                                       |[PINK HAPPY BIRTHDAY BUNTING]       |0.6717557251908397|43.71475699106218 |0.010193050193050193|
|[POPPY'S PLAYHOUSE BEDROOM ]                                        |[POPPY'S PLAYHOUSE KITCHEN]         |0.7370892018779343|43.387750746905674|0.012123552123552124|
|[POPPY'S PLAYHOUSE KITCHEN]                                         |[POPPY'S PLAYHOUSE BEDROOM ]        |0.7136363636363636|43.387750746905674|0.012123552123552124|
|[SET/6 RED SPOTTY PAPER PLATES, SET/20 RED RETROSPOT PAPER NAPKINS ]|[SET/6 RED SPOTTY PAPER CUPS]       |0.7162162162162162|43.3411214953271  |0.010231660231660231|
|[WOODEN STAR CHRISTMAS SCANDINAVIAN]                                |[WOODEN TREE CHRISTMAS SCANDINAVIAN]|0.5465116279069767|41.2672045562411  |0.010888030888030888|
|[WOODEN TREE CHRISTMAS SCANDINAVIAN]                                |[WOODEN STAR CHRISTMAS SCANDINAVIAN]|0.8221574344023324|41.2672045562411  |0.010888030888030888|
|[SET/6 RED SPOTTY PAPER PLATES]                                     |[SET/6 RED SPOTTY PAPER CUPS]       |0.6641366223908919|40.18957598113108 |0.013513513513513514|
|[SET/6 RED SPOTTY PAPER CUPS]                                       |[SET/6 RED SPOTTY PAPER PLATES]     |0.8177570093457944|40.18957598113107 |0.013513513513513514|
+--------------------------------------------------------------------+------------------------------------+------------------+------------------+--------------------+
only showing top 20 rows
```
<br>


## 四、结论
从关联规则可以发现，大部分关联规则中的项都是系列产品，购买某一产品的人，极可能购买该系列中的其他产品。