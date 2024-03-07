# GraphFrames实现方法



**· Edited by Rorschach**



## 一、数据准备

首先下载安装GraphFrames

```
cd /usr/local/spark
./bin/spark-shell --packages graphframes:graphframes:0.8.3-spark3.4-s_2.12
```



顺利安装并进入spark-shell

```
cd /usr/local/spark
bin/spark-shell
```

```
Spark session available as 'spark'.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.4.0
      /_/
         
Using Scala version 2.12.17 (Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_371)
Type in expressions to have them evaluated.
Type :help for more information.

```



创建顶点集 vertices

```
scala> val rdd = sc.parallelize(List(("a","Alice",24),("b","Bob",25),("c","Charlie",30),("d","David",29),("e","Esther",32),("f","Fanny",36),("g","Gabby",60)))
rdd: org.apache.spark.rdd.RDD[(String, String, Int)] = ParallelCollectionRDD[0] at parallelize at <console>:23

scala> val vertices = rdd.toDF("id","name","age")
vertices: org.apache.spark.sql.DataFrame = [id: string, name: string ... 1 more field]

```



查看 vertices

```
scala> vertices.show()

+---+-------+---+
| id|   name|age|
+---+-------+---+
|  a|  Alice| 24|
|  b|    Bob| 25|
|  c|Charlie| 30|
|  d|  David| 29|
|  e| Esther| 32|
|  f|  Fanny| 36|
|  g|  Gabby| 60|
+---+-------+---+
```



创建边集

```
scala> val rdd = sc.parallelize(List(("a","b","friend"),("b","c","follow"),("c","b","follow"),("f","c","follow"),("e","f","follow"),("e","d","friend"),("d","a","friend"),("a","e","friend")))

rdd: org.apache.spark.rdd.RDD[(String, String, String)] = ParallelCollectionRDD[4] at parallelize at <console>:23

```

```
scala> val edges = rdd.toDF("src","dst","relationship")

edges: org.apache.spark.sql.DataFrame = [src: string, dst: string ... 1 more field]
```



查看 vertices

```
scala> edges.show()

+---+---+------------+
|src|dst|relationship|
+---+---+------------+
|  a|  b|      friend|
|  b|  c|      follow|
|  c|  b|      follow|
|  f|  c|      follow|
|  e|  f|      follow|
|  e|  d|      friend|
|  d|  a|      friend|
|  a|  e|      friend|
+---+---+------------+
```



构建 graph

```
scala> import org.graphframes._
import org.graphframes._

scala> val graph = GraphFrame(vertices,edges)
graph: org.graphframes.GraphFrame = GraphFrame(v:[id: string, name: string ... 1 more field], e:[src: string, dst: string ... 1 more field])
```



## 二、广度优先算法



创建广度优先算法

```
scala> val paths = graph.bfs.fromExpr("name = 'Esther'").toExpr("age < 32")
paths: org.graphframes.lib.BFS = org.graphframes.lib.BFS@4a126898
```



查看结果

```
scala> paths.run().show()

+---------------+--------------+--------------+
|           from|            e0|            to|
+---------------+--------------+--------------+
|{e, Esther, 32}|{e, d, friend}|{d, David, 29}|
+---------------+--------------+--------------+
```



## 三、最短路径



创建最短路径算法

```
scala> val results = graph.shortestPaths.landmarks(Seq("a","d"))
results: org.graphframes.lib.ShortestPaths = org.graphframes.lib.ShortestPaths@472bb137

```



查看结果

```
scala> results.run().select("id","distances").show()

+---+----------------+
| id|       distances|
+---+----------------+
|  g|              {}|
|  f|              {}|
|  e|{a -> 2, d -> 1}|
|  d|{a -> 1, d -> 0}|
|  c|              {}|
|  b|              {}|
|  a|{a -> 0, d -> 2}|
+---+----------------+
```



## 四、三角形计数



创建三角形计数

```
scala> val results = graph.triangleCount
results: org.graphframes.lib.TriangleCount = org.graphframes.lib.TriangleCount@41f24cc6
```



查看结果

```
scala> results.run().show()

+-----+---+-------+---+
|count| id|   name|age|
+-----+---+-------+---+
|    1|  a|  Alice| 24|
|    0|  b|    Bob| 25|
|    0|  c|Charlie| 30|
|    1|  d|  David| 29|
|    1|  e| Esther| 32|
|    0|  f|  Fanny| 36|
|    0|  g|  Gabby| 60|
+-----+---+-------+---+
```



## 五、连通分量



设置检查点目录并创建连通分量

```
scala> sc.setCheckpointDir("/tmp/checkpoint")
scala> val result = graph.connectedComponents

result: org.graphframes.lib.ConnectedComponents = org.graphframes.lib.ConnectedComponents@60b84d6f
```



查看结果

```
scala> result.run().select("id","component").orderBy("component").show()

+---+------------+
| id|   component|
+---+------------+
|  g|146028888064|
|  b|412316860416|
|  b|412316860416|
|  f|412316860416|
|  d|412316860416|
|  e|412316860416|
|  a|412316860416|
+---+------------+
```





## 六、标签传播算法



创建标签传播算法

```
scala> val result = graph.labelPropagation.maxIter(5)
result: org.graphframes.lib.LabelPropagation = org.graphframes.lib.LabelPropagation@5e206402
```



查看结果

```
scala> result.run().select("id","label").orderBy("label").show()

+---+-------------+
| id|        label|
+---+-------------+
|  g| 146028888064|
|  f|1047972020224|
|  b|1047972020224|
|  c|1382979469312|
|  a|1382979469312|
|  e|1460288880640|
|  d|1460288880640|
+---+-------------+
```



## 七、PageRank算法



### 动态 PageRank 算法



创建 PageRank 算法

```
scala> val results = graph.pageRank.resetProbability(0.15).tol(0.01)
results: org.graphframes.lib.PageRank = org.graphframes.lib.PageRank@6fe1643

```



查看结果

```
scala> results.run().vertices.select("id","pagerank").show()

+---+-------------------+
| id|           pagerank|
+---+-------------------+
|  g| 0.1799821386239711|
|  f| 0.3283606792049851|
|  e|0.37085233187676075|
|  d| 0.3283606792049851|
|  c| 2.6878300011606218|
|  b|  2.655507832863289|
|  a|0.44910633706538744|
+---+-------------------+

scala> results.run().edges.select("src","dst","weight").show()

+---+---+------+
|src|dst|weight|
+---+---+------+
|  f|  c|   1.0|
|  e|  f|   0.5|
|  e|  d|   0.5|
|  d|  a|   1.0|
|  c|  b|   1.0|
|  b|  c|   1.0|
|  a|  e|   0.5|
|  a|  b|   0.5|
+---+---+------+
```



### 静态 PageRank 算法



创建 PageRank 算法

```
scala> val results = graph.pageRank.resetProbability(0.15).maxIter(10)
results: org.graphframes.lib.PageRank = org.graphframes.lib.PageRank@40d2d73d
```



查看结果

```
scala> results.run().vertices.select("id","pagerank").show()

+---+-------------------+
| id|           pagerank|
+---+-------------------+
|  g|0.17073170731707318|
|  f|0.32504910549694244|
|  e| 0.3613490987992571|
|  d|0.32504910549694244|
|  c| 2.6667877057849627|
|  b| 2.7025217677349773|
|  a| 0.4485115093698443|
+---+-------------------+

scala> results.run().edges.select("src","dst","weight").show()
+---+---+------+
|src|dst|weight|
+---+---+------+
|  f|  c|   1.0|
|  e|  f|   0.5|
|  e|  d|   0.5|
|  d|  a|   1.0|
|  c|  b|   1.0|
|  b|  c|   1.0|
|  a|  e|   0.5|
|  a|  b|   0.5|
+---+---+------+
```







