# MapReduce_JinYong

金庸小说人物关系分析



## 1 数据说明

本实验数据集为金庸武侠小说文本。为方便处理，每句话都被处理为单独一行。另外将小说中所有人名存储在一个文件中，作为分词阶段的停词表。

```
// 小说文件存在 wuxia_novels 文件夹中
// 人名存在 People_List_unique.txt 文件中
```



## 2 实验准备

### 2.1 数据获取

使用命令行解压缩文件

```
cd ~/Downloads
unzip 金庸.zip
```



### 2.2 文件位置

将文件置于 

```
/home/hadoop/Downloads/JinYong
```

文件夹中，方便后续使用。



## 3 任务1 数据预处理：中文分词

### 3.1 任务目标

```
输入:
(金庸03连城诀.txt 中的某一段内容)	\
狄云和戚芳一走到万家大宅之前，瞧见那高墙朱门、挂灯结彩的气派，心中都是暗自嘀咕。戚芳紧紧拉住了父亲的衣袖。戚长发正待向门公询问，忽见卜垣从门里出来，心中一喜，叫道:“卜贤侄，我来啦。”

输出:
狄云 戚芳 戚芳 戚长发 卜垣 	1
```



数据预处理阶段需要将金庸小说中的人名提取出来。利用people_name_list.txt文件中的分词表，匹配人名即可。这里使用ansj进行分词。

处理每一行文本时，需要将其中的人名提取出来，将这一部分的逻辑抽象为一个Ansj类。该类的功能有ansj包实现，只需要将分词表输入DicLibrary中，在处理每一行的字符时，调用DicAnalysis.parse(str)方法得到句子中的人名，以空格间隔输出即可。



新建 **IDEA** 项目 

### 3.2 依赖项 pom.xml



```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">

    <groupId>MapReduceJinYong</groupId>
    <artifactId>MapReduceJinYong</artifactId>
    <modelVersion>4.0.0</modelVersion>
    <name>MapReduce Project</name>
    <packaging>jar</packaging>
    <version>1.0</version>


    <properties>
        <maven.compiler.source>1.8</maven.compiler.source>
        <maven.compiler.target>1.8</maven.compiler.target>
        <spark.version>3.5.0</spark.version>
        <scala.version>2.12.18</scala.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.ansj</groupId>
            <artifactId>ansj_seg</artifactId>
            <version>5.1.6</version>
        </dependency>

        <dependency>
            <groupId>org.ansj</groupId>
            <artifactId>tree_split</artifactId>
            <version>1.5</version>
        </dependency>

        <dependency>
            <groupId>org.nlpcn</groupId>
            <artifactId>nlp-lang</artifactId>
            <version>1.7.9</version>
        </dependency>

        <!-- https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-client -->
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-client</artifactId>
            <version>3.3.5</version>
        </dependency>

        <!-- https://mvnrepository.com/artifact/junit/junit -->
        <dependency>
            <groupId>junit</groupId>
            <artifactId>junit</artifactId>
            <version>4.8.1</version>
            <scope>test</scope>
        </dependency>

        <!-- https://mvnrepository.com/artifact/org.slf4j/slf4j-log4j12 -->
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-simple</artifactId>
            <version>1.7.25</version>
            <scope>compile</scope>
        </dependency>

    </dependencies>

    <build>
        <plugins>
            <plugin>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.6.0</version>
                <configuration>
                    <source>1.8</source>
                    <target>1.8</target>
                </configuration>
            </plugin>

            <plugin>
                <artifactId>maven-assembly-plugin</artifactId>
                <configuration>
                    <descriptorRefs>
                        <descriptorRef>jar-with-dependencies</descriptorRef>
                    </descriptorRefs>
                </configuration>
                <executions>
                    <execution>
                        <id>make-assembly</id>
                        <phase>package</phase>
                        <goals>
                            <goal>single</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>

        </plugins>

    </build>

</project>
```



### 3.3  Ansj.java



```java
package org.example;

/**
 * Ansj.java
 */

import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.library.DicLibrary;
import org.ansj.splitWord.analysis.DicAnalysis;

import java.io.BufferedReader;
import java.io.IOException;

/**
 * Load a user dict and get tokens appeared in that dict from a string.
 */
public class Ansj{
    private static boolean isInited = false;
    /**
     * Insert people's name into user dictionary.
     * @param bufferedReader Scanner contains people's names.
     */
    Ansj(BufferedReader bufferedReader) throws IOException {
        if(isInited)
            return;
        String name;
        while((name = bufferedReader.readLine()) != null){
            DicLibrary.insert(DicLibrary.DEFAULT, name);
        }
        isInited = true;
    }

    /**
     * Split line into tokens and return tokens which appeared in userdict.
     * @param line the line to parse.
     * @return empty String is length equals 0, else return tokens separated by space.
     */
    String getNames(String line) {
        Result parse = DicAnalysis.parse(line);
        StringBuilder result = new StringBuilder();
        for(Term t: parse) {
            if(t.getNatureStr().equals("userDefine")) {
                // retain user defined tokens.
                result.append(t.getName()).append(" ");
            }
        }
        if(result.length() != 0) // erase last space because of QPZ
            result.deleteCharAt(result.length() - 1);
        return result.toString();
    }
}

```



### 3.4  WordSplit.java

接着编写MapReduce的逻辑，有了上述的Ansj类，这一部分的任务与WordCount相似，在map阶段将文本每一行处理为以空格间隔的若干人名，输出如下的键值对：

```
<"甲 乙 丙 丁", 1>
```



在reduce阶段，统计具有相同的key的键值对的个数。在接下来的分析中，只关心句子中的人名，人名完全相同的的句子可以被认为是重复的，reduce阶段实际上是将重复的句子进行合并。



```java
package org.example;

/**
 * WordSplit.java
 */
import org.example.Ansj;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;

/**
 * Stage 1.
 */
public class WordSplit {
    public static class WordSplitMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private Ansj ansj;
        private Text outputKey = new Text();
        private IntWritable outputValue = new IntWritable(1);

        @Override
        protected void setup(Context context) throws IOException {
            FileSystem fs = FileSystem.get(context.getConfiguration());
            // must use fs to open the cache file in hdfs.
            ansj = new Ansj(new BufferedReader(new InputStreamReader(fs.open(new Path(context.getCacheFiles()[0])))));
        }


        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String result = ansj.getNames(value.toString());
            if(!result.isEmpty()) {
                outputKey.set(result);
                context.write(outputKey, outputValue);
            }
        }
    }

    // combiner and reducer.
    public static class WordSplitReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Word_Split_JinYong");

        job.setJarByClass(WordSplit.class);
        job.setMapperClass(WordSplitMapper.class);
        job.setCombinerClass(WordSplitReducer.class);
        job.setReducerClass(WordSplitReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.addCacheFile(new Path("file:///home/hadoop/Downloads/JinYong/People_List_unique.txt").toUri());
        FileInputFormat.addInputPath(job, new Path("file:///home/hadoop/Downloads/JinYong//wuxia_novels"));
        FileOutputFormat.setOutputPath(job, new Path("file:///home/hadoop/Downloads/JinYong_output"));

        if (!job.waitForCompletion(true))
            throw new IllegalStateException(job.getJobName() +  " failed.");
    }
}
```



### 3.5 应用程序打包

打包后查看文件结构

```
cd /home/hadoop/idea/MapReduce_JinYong
(base) hadoop@hadoop-virtual-machine:~/idea/MapReduce_JinYong$ find .
```



```
.
./.idea
./.idea/misc.xml
./.idea/compiler.xml
./.idea/workspace.xml
./.idea/jarRepositories.xml
./.idea/encodings.xml
./.idea/.gitignore
./target
./target/MapReduceJinYong-1.0-jar-with-dependencies.jar
./target/maven-archiver
./target/maven-archiver/pom.properties
./target/classes
./target/classes/org
./target/classes/org/example
./target/classes/org/example/WordSplit.class
./target/classes/org/example/Ansj.class
./target/classes/org/example/WordSplit$WordSplitMapper.class
./target/classes/org/example/WordSplit$WordSplitReducer.class
./target/maven-status
./target/maven-status/maven-compiler-plugin
./target/maven-status/maven-compiler-plugin/testCompile
./target/maven-status/maven-compiler-plugin/testCompile/default-testCompile
./target/maven-status/maven-compiler-plugin/testCompile/default-testCompile/inputFiles.lst
./target/maven-status/maven-compiler-plugin/compile
./target/maven-status/maven-compiler-plugin/compile/default-compile
./target/maven-status/maven-compiler-plugin/compile/default-compile/inputFiles.lst
./target/maven-status/maven-compiler-plugin/compile/default-compile/createdFiles.lst
./target/archive-tmp
./target/generated-sources
./target/generated-sources/annotations
./target/MapReduceJinYong-1.0.jar
./src
./src/main
./src/main/java
./src/main/java/org
./src/main/java/org/example
./src/main/java/org/example/Ansj.java
./src/main/java/org/example/WordSplit.java
./src/main/resources
./src/test
./src/test/java
./pom.xml
./.gitignore
```





### 3.6 运行程序

运行程序可以在 **~/Downloads/JinYong_output** 中找到结果

```
 (base) hadoop@hadoop-virtual-machine:~$ cd ~/Downloads/JinYong_output
(base) hadoop@hadoop-virtual-machine:~/Downloads/JinYong_output$ ls
part-r-00000  _SUCCESS
```



其中 **part-r-00000** 即为结果



### 3.7 查看结果

```
(base) hadoop@hadoop-virtual-machine:~/Downloads/JinYong_output$ head -n 5 part-r-00000 
```

```
一灯大师	10
一灯大师 一灯大师	1
一灯大师 一灯大师 农夫 农夫 农夫 渔人 渔人	1
一灯大师 一灯大师 周伯通	1
一灯大师 农夫 樵子 农夫 渔人 渔人 渔人 农夫 樵子 农夫	1
```



至此，任务1完成！



## 4 任务2 特征抽取：任务同现统计

### 4.1 任务目标

任务2 以 任务1 为基础

```
输入：
狄云 戚芳 戚芳 戚长发 卜垣		1\
戚芳 卜垣 卜垣	1

输出：
<狄云，戚芳>		1\
<狄云，戚长发>	1\
<狄云，卜垣>		1\
<戚芳，狄云>		1\
<戚芳，戚长发>	1\
<戚芳，卜垣> 	2\
<戚长发，狄云>	1\
<戚长发，戚芳>	1\
<戚长发，卜垣>	1\
<卜垣，狄云>		1\
<卜垣，戚芳>		2\
<卜垣，戚长发>	1
```

 

如果两个人在原文的同意段落中出现，则认为两个人发生了一次同现关系。我们需要对人物之间的同现关系次数进行统计，同现关系次数越多，说明两人关系越密切。



在 map 阶段，首先将一个句子中的重复人名去除，接着对其中的每一对人名，输出如下键值对：

```
<"张三 李四", 1>
```



在 reduce 阶段，将键值相同的键值对进行归并，求和，就有了每一对任务同现的次数。



### 4.2 Cooccurrence.java

```java
/**
 * Cooccurrence.java
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

/**
 * Stage 2.
 */
public class Cooccurrence {
    public static class CooccurrenceMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private Text outputKey = new Text();
        private IntWritable outputValue = new IntWritable();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString(), "\t");
            String tokenKey = tokenizer.nextToken();
            String tokenValue = tokenizer.nextToken();
            tokenizer = new StringTokenizer(tokenKey, " ");
            List<String> names = new ArrayList<>();
            while (tokenizer.hasMoreTokens()){
                names.add(tokenizer.nextToken());
            }

            // remove deplicated names
            Collections.sort(names);
            String before = "", current;
            ListIterator<String> iter = names.listIterator();
            while (iter.hasNext()){
                current = iter.next();
                if (current.equals(before))
                    iter.remove();
                else
                    before = current;
            }

            // emit all name pairs in a line
            outputValue.set(Integer.parseInt(tokenValue));
            for (String i : names) {
                for (String j : names) {
                    if (!i.equals(j)) {
                        outputKey.set(i + " " + j);
                        context.write(outputKey, outputValue);
                    }
                }
            }
        }
    }

    //combiner and reducer.
    public static class CooccurrenceReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Co-occurrence");
        job.setJarByClass(Cooccurrence.class);
        job.setMapperClass(CooccurrenceMapper.class);
        job.setCombinerClass(CooccurrenceReducer.class);
        job.setReducerClass(CooccurrenceReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("file:///home/hadoop/Downloads/JinYong_output/part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path("file:///home/hadoop/Downloads/Cooccrence_output"));
        if (!job.waitForCompletion(true))
            throw new IllegalStateException(job.getJobName() + " failed.");
    }
}
```



### 4.3 应用程序打包

打包后查看文件结构

```
cd /home/hadoop/idea/Cooccurrence_JinYong
(base) hadoop@hadoop-virtual-machine:~/idea/Cooccurrence_JinYong$ find .
```

```
.
./.idea
./.idea/misc.xml
./.idea/compiler.xml
./.idea/workspace.xml
./.idea/jarRepositories.xml
./.idea/encodings.xml
./.idea/.gitignore
./target
./target/MapReduceJinYong-1.0-jar-with-dependencies.jar
./target/maven-archiver
./target/maven-archiver/pom.properties
./target/classes
./target/classes/Cooccurrence$CooccurrenceMapper.class
./target/classes/Cooccurrence.class
./target/classes/Cooccurrence$CooccurrenceReducer.class
./target/maven-status
./target/maven-status/maven-compiler-plugin
./target/maven-status/maven-compiler-plugin/testCompile
./target/maven-status/maven-compiler-plugin/testCompile/default-testCompile
./target/maven-status/maven-compiler-plugin/testCompile/default-testCompile/inputFiles.lst
./target/maven-status/maven-compiler-plugin/compile
./target/maven-status/maven-compiler-plugin/compile/default-compile
./target/maven-status/maven-compiler-plugin/compile/default-compile/inputFiles.lst
./target/maven-status/maven-compiler-plugin/compile/default-compile/createdFiles.lst
./target/archive-tmp
./target/generated-sources
./target/generated-sources/annotations
./target/MapReduceJinYong-1.0.jar
./src
./src/main
./src/main/java
./src/main/java/Cooccurrence.java
./src/main/resources
./src/test
./src/test/java
./pom.xml
./.gitignore
```



### 4.4 运行程序

运行程序可以在 **~/Downloads/Cooccurrence_output** 中找到结果

```
 (base) hadoop@hadoop-virtual-machine:~$ cd ~/Downloads/Cooccurrence_output
(base) hadoop@hadoop-virtual-machine:~/Downloads/Cooccurrence_output$ ls
part-r-00000  _SUCCESS
```



其中 **part-r-00000** 即为结果



### 4.5 查看结果

```
(base) hadoop@hadoop-virtual-machine:~/Downloads/Cooccrence_output$ head -n 5 part-r-00000
一灯大师 上官	1
一灯大师 丘处机	5
一灯大师 乔寨主	1
一灯大师 农夫	17
一灯大师 华筝	1
```



至此，任务2完成！



## 5 任务3 特征处理：人物关系图构建与特征一体化

### 5.1 任务目标

任务3 以 任务2 为基础

```
输入:
<狄云，戚芳>		1\
<狄云，戚长发>	1\
<狄云，卜垣>		1\
<戚芳，狄云>		1\
<戚芳，戚长发> 	1\
<戚芳，卜垣> 	2\
<戚长发，狄云>	1\
<戚长发，戚芳>	1\
<戚长发，卜垣>	1\
<卜垣，狄云> 	1\
<卜垣，戚芳> 	2\
<卜垣，戚长发> 	1\

输出:
狄云[戚芳,0.33333|戚长发，0.333333|卜垣0.333333]\
戚芳[狄云,0.25|戚长发，0.25|卜垣0.5]\
戚长发[狄云,0.33333|戚芳，0.333333|卜垣.333333]\
卜垣[狄云0.25|戚芳,0.5|戚长发，0.25]
```



当获取了人物之间的共现关系之后，我们就可以根据共现关系，生成人物之间的关系图了。人物关系图使用邻接表的形式表示，以方便后面的PageRank计算。在人物关系图中，人物是顶点，人物之间的互动关系是边。人物之间的互动关系靠人物之间的共现关系确定。如果两个人之间具有共现关系，则两个人之间就具有一条边。两人之间的共现频率体现出两人关系的密切程度，反映到共现关系图上就是边的权重。

具体来说，计算边的权重时，首先是将统计出的人物共现次数结果，转换成邻接表的形式表示:每一行表示一个邻接关系。

“戚芳[狄云,0.25|戚长发，0.25 |卜垣0.5]” 表示了顶点“戚芳”，有三个邻接点，分别是“狄云”、“戚长发和“卜垣”，对应三条邻接边，每条邻接边上分别具有不同的权重。这个邻接边的权重是依靠某个人与其他人共现的“频率“得到的，以“戚芳”为例，她分别与三个人 (“狄云”共现1次、“戚长发”，共现1次、“卜垣“共现2次)有共现关系，则戚芳与三个人共现的“频率”分别为1/(1+1+2)= 0.25，1/(1+1+2)= 0.252/(1+1+2)=0.5。这三个“频率”值对应于三条边的权重。通过这种归一化，我们确保了某个顶点的出边权重的和为1。



任务3 的代码如下。在 map 阶段，简单地将每一行输入中的人名对与同现次数分离出来。输出的键值对形如:

```
<"张三 李四", 1>
```



然而，为了能在reduce阶段统计每个邻接边的权重，我们希望所有以“张三”开头的pair被发送给同一个reducer，即我们希望在将键值对分发给reducer时，以人名对中的第一个人名为key，而非以整个人名对为key。因此，我们需要重写Partitioner。如getPartition方法中所写，我们将outputKey重新设置为pair中的第一个人名。

在reduce阶段，尽管Key值相同的pair都被传入了同一个Reducer，但同一个Reducer仍然可能收到不同的key。好消息是，由于调用是按key的大小顺序传入的，所以我们只要将当前已经收到的邻接节点保存在内存中，等接受到一个不一样的key值时，说明之前的节点的所有邻接节点都已接收完毕，这时我们再对之前节点的所有临边计算权值即可。



### 5.2 Normalization.java

```java
/**
 * Normalization.java
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.IOException;
import java.util.*;

/**
 * Stage 3.
 */

public class Normalization {
    public static class NormalizationMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private Text outputKey = new Text();
        private IntWritable outputValue = new IntWritable();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), "\t");
            String first = itr.nextToken();
            String second = itr.nextToken();
            outputKey.set(first);
            outputValue.set(Integer.parseInt(second));
            context.write(outputKey, outputValue);
        }
    }

    public static class NormalizationPartitioner
            extends HashPartitioner<Text, IntWritable> {
        private Text outputKey = new Text();

        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            StringTokenizer tokenizer = new StringTokenizer(key.toString());
            outputKey.set(tokenizer.nextToken());
            return super.getPartition(outputKey, value, numPartitions);
        }
    }

    //only reducer because al1 keys are unique and combiner is not needed.
    public static class NormalizationReducer extends Reducer<Text, IntWritable, Text, Text> {
        class Tuple {
            String name;
            int count;

            Tuple(String n, int c) {
                name = n;
                count = c;
            }
        }

        List<Tuple> tmpValue = new ArrayList<>();
        private String preKey = "";
        private int sum = 0;
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        private void contextWrite(Context context) throws IOException, InterruptedException {
            outputKey.set(preKey);
            StringBuilder builder = new StringBuilder();
            builder.append("[");

            for (Tuple t : tmpValue) {
                builder.append(t.name).append(",").append((double) t.count / sum).append("|");
            }

            builder.deleteCharAt(builder.length() - 1);
            builder.append("]");

            outputValue.set(builder.toString());
            context.write(outputKey, outputValue);

            sum = 0;
            tmpValue.clear();
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            Iterator<IntWritable> it = values.iterator();
            int tmpSum = 0;

            while (it.hasNext())
                tmpSum += it.next().get();

            StringTokenizer tokenizer = new StringTokenizer(key.toString(), " ");
            String first = tokenizer.nextToken();
            String second = tokenizer.nextToken();

            if (!first.equals(preKey)) {
                if (!preKey.isEmpty())
                    contextWrite(context);
                preKey = first;
            }

            sum += tmpSum;
            tmpValue.add(new Tuple(second, tmpSum));
        }

        @Override
        // Don't omit the last one
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            if (!preKey.isEmpty())
                contextWrite(context);
        }

        public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Normalization");
            job.setJarByClass(Normalization.class);
            job.setMapperClass(NormalizationMapper.class);
            job.setReducerClass(NormalizationReducer.class);
            job.setPartitionerClass(NormalizationPartitioner.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job, new Path("file:///home/hadoop/Downloads/Cooccrence_output/part-r-00000"));
            FileOutputFormat.setOutputPath(job, new Path("file:///home/hadoop/Downloads/Normalization_output"));
            if (!job.waitForCompletion(true))
                throw new IllegalStateException(job.getJobName() + " failed.");
        }
    }
}
```



### 5.3 应用程序打包

```
cd /home/hadoop/idea/Normalization_JinYong
(base) hadoop@hadoop-virtual-machine:~/idea/Normalization_JinYong$ find .
```

```
.
./.idea
./.idea/misc.xml
./.idea/compiler.xml
./.idea/workspace.xml
./.idea/jarRepositories.xml
./.idea/encodings.xml
./.idea/.gitignore
./target
./target/classes
./target/classes/Normalization.class
./target/classes/Normalization$NormalizationPartitioner.class
./target/classes/Normalization$NormalizationReducer$Tuple.class
./target/classes/Normalization$NormalizationMapper.class
./target/classes/Normalization$NormalizationReducer.class
./target/generated-sources
./target/generated-sources/annotations
./src
./src/main
./src/main/java
./src/main/java/Normalization.java
./src/main/resources
./src/test
./src/test/java
./pom.xml
./.gitignore
```



### 5.4 运行程序

运行程序可以在 **~/Downloads/Normalization_output** 中找到结果

```
 (base) hadoop@hadoop-virtual-machine:~$ cd ~/Downloads/Normalization_output
(base) hadoop@hadoop-virtual-machine:~/Downloads/Normalization_output$ ls
part-r-00000  _SUCCESS
```



其中 **part-r-00000** 即为结果



### 5.5 查看结果

```
(base) hadoop@hadoop-virtual-machine:~/Downloads/Normalization_output$ head -n 1 part-r-00000
```

```
一灯大师	[上官,0.002347417840375587|丘处机,0.011737089201877934|乔寨主,0.002347417840375587|农夫,0.03990610328638498|华筝,0.002347417840375587|卫璧,0.004694835680751174|吕文德,0.002347417840375587|周伯通,0.06572769953051644|哑巴,0.002347417840375587|哑梢公,0.002347417840375587|大汉,0.002347417840375587|天竺僧,0.002347417840375587|天竺僧人,0.007042253521126761|完颜萍,0.004694835680751174|小沙弥,0.007042253521126761|小龙女,0.018779342723004695|尹克西,0.002347417840375587|尼摩星,0.007042253521126761|张无忌,0.004694835680751174|无色,0.002347417840375587|明月,0.002347417840375587|朱九真,0.004694835680751174|朱子柳,0.03051643192488263|朱长龄,0.002347417840375587|李莫愁,0.011737089201877934|杨康,0.007042253521126761|杨过,0.07981220657276995|柯镇恶,0.004694835680751174|梅超风,0.002347417840375587|樵子,0.02112676056338028|欧阳克,0.007042253521126761|武三娘,0.004694835680751174|武三通,0.03286384976525822|武修文,0.007042253521126761|武敦儒,0.004694835680751174|武青婴,0.004694835680751174|汉子,0.004694835680751174|洪七公,0.025821596244131457|渔人,0.028169014084507043|潇湘子,0.002347417840375587|点苍渔隐,0.007042253521126761|琴儿,0.002347417840375587|穆念慈,0.002347417840375587|老头子,0.002347417840375587|耶律燕,0.007042253521126761|耶律齐,0.011737089201877934|裘千丈,0.002347417840375587|裘千仞,0.03051643192488263|裘千尺,0.014084507042253521|觉远,0.002347417840375587|觉远大师,0.002347417840375587|达尔巴,0.004694835680751174|郝大通,0.004694835680751174|郭芙,0.009389671361502348|郭襄,0.04225352112676056|郭靖,0.11737089201877934|金轮法王,0.009389671361502348|陆乘风,0.002347417840375587|陆无双,0.01643192488262911|霍都,0.007042253521126761|马钰,0.004694835680751174|鲁有脚,0.009389671361502348|黄药师,0.035211267605633804|黄蓉,0.16901408450704225]
```



至此，任务3 完成！



## 6 任务4 数据分析：基于人物关系图的 PageRank 计算

### 6.1 任务目标

任务4 基于 任务3 完成

```
输入：
任务3 的输出

输出：
人物的 PageRank 值 
```



在给出人物关系图之后，我们就可以对人物关系图进行一个数据分析。其中一个典型的分析任务是PageRank值计算。通过计算PageRank，我们就可以定量地确定金庸武侠江湖中的“主角”们是谁。

PageRank是一个经典的算法，这里我们主要关注如何使用MapReduce实现并行化的算法。代码如下所示，其中涉及了三个类: PageRank、PageRanklter、PageRankViewer。下面一一介绍各个类的功能：

- PageRank类作为Driver类，将会调用PageRanklter和PageRankViewer的main函数。
- PageRanklter是执行Pagerank计算的主体，需要进行多次迭代。具体而言，lter主要分为map和reduce两个函数，map函数会读取到每一个人名及其对应的pagerank值和关系列表，对于关系列表中的每一个其他人物，都会发射一个对应的键值对，同时还会发射一个包含该人名以及其关系列表的键值对。reduce函数接受了每一个人名对应的键值对之后，对于每一个人名，计算更新之后的pagerank值，记录其关系列表，然后输出到文件中。本次reduce的输出将会作为下一轮map的输入。
- PageRankViewer负责将Pagerank计算的结果整理排序。Viewer会借助MapReduce的机制进行排序，具体而言，map函数中会把pagerank值作为key，人名作为value，sort过程会自动将所有键值对按照key的大小排序，reduce输出时再将键值对反过来输出即可。



### 6.2 PageRankIter.java

```java
/**

 * PageRankIter.java
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

/**

 * Compute Page Rank.
 * INPUT: name (rank) [TAB] name2,0.75|name3,0.25
 * OUTPUT: name rank [TAB] name2,0.75|name3,0.25
 */

public class PageRankIter {
    public static class PageRankIterMapper
            extends Mapper<Object, Text, Text, Text> {

        private double d = 0.85;
        private int N = 1283;
        private Text outputKey = new Text();
        private Text outputvalue = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            double curPR;
            StringTokenizer itr = new StringTokenizer(value.toString(), "\t[]");
            String first = itr.nextToken();
            String second = itr.nextToken();

            String[] farray = first.split(" ");
            String name = farray[0];

            if (farray.length == 1) {
                curPR = (1 - d) / N;
            } else {
                curPR = Double.valueOf(farray[1]);
            }


            String[] sarray = second.split("\\|");

            for (String s : sarray) {
                double uPR;
                String[] itemList = s.split(",");
                String u = itemList[0];
                double weight =  Double.valueOf(itemList[1]);
                uPR = weight * curPR;
                String suPR = String.valueOf(uPR);
                outputKey.set(u);
                outputvalue.set(suPR);
                context.write(outputKey, outputvalue);
            }
            outputKey.set(name);
            outputvalue.set("#" + second);
            context.write(outputKey, outputvalue);
        }

        public static class PageRankIterReducer
                extends Reducer<Text, Text, Text, Text> {

            private double d = 0.85;
            private int N = 1283;
            private Text outputKey = new Text();
            private Text outputvalue = new Text();

            @Override
            public void reduce(Text key, Iterable<Text> values, Context context)
                    throws IOException, InterruptedException {
                Iterator<Text> it = values.iterator();
                String namelist = "";
                String name = key.toString();
                Double PR = (1 - d) / N;
                while (it.hasNext()) {
                    String str = it.next().toString();
                    if (str.charAt(0) == '#') {
                        namelist = str.substring(1, str.length());
                    }
                    else {
                        PR += d * Double.valueOf(str);
                    }
                    outputKey.set(name + " " + String.valueOf(PR));
                    outputvalue.set(namelist);
                    context.write(outputKey, outputvalue);
                }
            }

            public static void main(String[] args) throws Exception {
                Configuration conf = new Configuration();
                Job job = Job.getInstance(conf, "Page Rank Iter");
                job.setJarByClass(PageRankIter.class);
                job.setMapperClass(PageRankIterMapper.class);
                job.setReducerClass(PageRankIterReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                FileInputFormat.addInputPath(job, new Path("file:///home/hadoop/Downloads/Normalization_output/part-r-00000"));
                FileOutputFormat.setOutputPath(job, new Path("file:///home/hadoop/Downloads/PageRankIter_output"));
                if (!job.waitForCompletion(true)) {
                    throw new IllegalStateException(job.getJobName() + " failed.");
                }
            }
        }
    }

}
```



### 6.3 应用程序打包

```
cd /home/hadoop/idea/PageRankIter_JinYong
(base) hadoop@hadoop-virtual-machine:~/idea/PageRankIter_JinYong$ find .
```

```
.
./.idea
./.idea/misc.xml
./.idea/compiler.xml
./.idea/workspace.xml
./.idea/jarRepositories.xml
./.idea/encodings.xml
./.idea/.gitignore
./target
./target/MapReduceJinYong-1.0-jar-with-dependencies.jar
./target/maven-archiver
./target/maven-archiver/pom.properties
./target/classes
./target/classes/PageRankIter$PageRankIterMapper.class
./target/classes/PageRankIter.class
./target/classes/PageRankIter$PageRankIterMapper$PageRankIterReducer.class
./target/maven-status
./target/maven-status/maven-compiler-plugin
./target/maven-status/maven-compiler-plugin/testCompile
./target/maven-status/maven-compiler-plugin/testCompile/default-testCompile
./target/maven-status/maven-compiler-plugin/testCompile/default-testCompile/inputFiles.lst
./target/maven-status/maven-compiler-plugin/compile
./target/maven-status/maven-compiler-plugin/compile/default-compile
./target/maven-status/maven-compiler-plugin/compile/default-compile/inputFiles.lst
./target/maven-status/maven-compiler-plugin/compile/default-compile/createdFiles.lst
./target/archive-tmp
./target/generated-sources
./target/generated-sources/annotations
./target/MapReduceJinYong-1.0.jar
./src
./src/main
./src/main/java
./src/main/java/PageRankIter.java
./src/main/resources
./src/test
./src/test/java
./pom.xml
./.gitignore
```



### 6.4 运行程序

运行程序可以在 **~/Downloads/PageRankIter_output** 中找到结果

```
 (base) hadoop@hadoop-virtual-machine:~$ cd ~/Downloads/PageRankIter_output
(base) hadoop@hadoop-virtual-machine:~/Downloads/PageRankIter_output$ ls
part-r-00000  _SUCCESS
```



其中 **part-r-00000** 即为结果



### 6.5 查看结果

```
(base) hadoop@hadoop-virtual-machine:~/Downloads/PageRankIter_output$ head -n 5 part-r-00000
```

```
一灯大师 1.1751213740386332E-4	
一灯大师 1.18388051727264E-4	
一灯大师 1.2170060044121567E-4	
一灯大师 1.233993433714473E-4	
一灯大师 1.240662994883503E-4
```



至此，任务4-1 完成！



### 6.6 PageRankViewer.java

```java
/** 
 * PageRankViewer.java
 */

/**
 * Retain the first two columns of the last result of pageRankiter.
 * And sort by rank desc.
 * INPUT: name rank [TAB] ...
 * OUTPUT: name [TAB] rank
 */

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRankViewer {
    public static class PageRankViewerMapper extends Mapper<Object, Text, DoubleWritable, Text> {
        private DoubleWritable outputKey = new DoubleWritable();
        private Text outputValue = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), " \t");
            String first = itr.nextToken();
            String[] sa = first.split(" ");
            String name = sa[0];
            String sPR = sa[1];
            double PR = Double.valueOf(sPR);
            outputKey.set(-1 * PR);
            outputValue.set(name);
            context.write(outputKey, outputValue);
        }
    }

    public static class PageRankViewerReducer extends Reducer<DoubleWritable, Text, Text, Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        @Override
        public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Iterator<Text> it = values.iterator();
            double PR = -1 * key.get();
            while (it.hasNext()) {
                String name = it.next().toString();
                outputKey.set(name);
                outputValue.set(String.valueOf(PR));
                context.write(outputKey, outputValue);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Page Rank Viewer " + args[2]);
        job.setJarByClass(PageRankViewer.class);
        job.setMapperClass(PageRankViewerMapper.class);
        job.setReducerClass(PageRankViewerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        if (!job.waitForCompletion(true)) {
            throw new IllegalStateException(job.getJobName() + " failed.");
        }
    }
}
```



### 6.7 应用程序打包

```
cd /home/hadoop/idea/PageRankViewer_JinYong
(base) hadoop@hadoop-virtual-machine:~/idea/PageRankViewer_JinYong$ find .
```

```
.
./.idea
./.idea/misc.xml
./.idea/compiler.xml
./.idea/workspace.xml
./.idea/jarRepositories.xml
./.idea/encodings.xml
./.idea/.gitignore
./target
./target/MapReduceJinYong-1.0-jar-with-dependencies.jar
./target/maven-archiver
./target/maven-archiver/pom.properties
./target/classes
./target/classes/PageRankViewer$PageRankViewerMapper.class
./target/classes/PageRankViewer$PageRankViewerReducer.class
./target/classes/PageRankViewer.class
./target/maven-status
./target/maven-status/maven-compiler-plugin
./target/maven-status/maven-compiler-plugin/testCompile
./target/maven-status/maven-compiler-plugin/testCompile/default-testCompile
./target/maven-status/maven-compiler-plugin/testCompile/default-testCompile/inputFiles.lst
./target/maven-status/maven-compiler-plugin/compile
./target/maven-status/maven-compiler-plugin/compile/default-compile
./target/maven-status/maven-compiler-plugin/compile/default-compile/inputFiles.lst
./target/maven-status/maven-compiler-plugin/compile/default-compile/createdFiles.lst
./target/archive-tmp
./target/generated-sources
./target/generated-sources/annotations
./target/MapReduceJinYong-1.0.jar
./src
./src/main
./src/main/java
./src/main/java/PageRankViewer.java
./src/main/resources
./src/test
./src/test/java
./pom.xml
./.gitignore
```



### 6.8 运行程序

运行程序可以在 **~/Downloads/PageRankViewer_output** 中找到结果

```
 (base) hadoop@hadoop-virtual-machine:~$ cd ~/Downloads/PageRankViewer_output
(base) hadoop@hadoop-virtual-machine:~/Downloads/PageRankViewer_output$ ls
part-r-00000  _SUCCESS
```



其中 **part-r-00000** 即为结果



### 6.9 查看结果

```
(base) hadoop@hadoop-virtual-machine:~/Downloads/PageRankViewer_output$ head -n 10 part-r-00000
```

```
韦小宝	0.00490520036994032
韦小宝	0.0048306680238764076
韦小宝	0.0048174178290206005
韦小宝	0.004771042147025277
韦小宝	0.004742810197758644
韦小宝	0.004738913081624583
韦小宝	0.004716206887175834
韦小宝	0.0046913627718211965
韦小宝	0.004654378004626591
韦小宝	0.004651954188494431
```



至此，任务4-2 完成！



## 7 任务5 标签传播分析

### 7.1 任务目标

任务5 也以 任务3 为基础

```
输入：
任务3 的输出 Normalization_output

输出：
人物的标签信息
```



标签传播(Label Propagation)是一种半监督图分析算法，他能发现网络图内部连接较为紧密的节点子集和对应的子图，也叫做社区(Community)。

根据各社区节点集合彼此是否有交集，社区可被分为非重叠型(disjoint)社区和重叠型(overlapping)社区。对于给定网络图，寻找社区结构的这个过程称为社区发现(Community Detection)。社区发现的过程其实是聚类的过程，标签传播算法就是一种不重叠社区发现的经典算法。
标签传播算法的基本思想是对某个顶点，取其邻居节点标签中数量最多的标签作为该顶点的标签(label)。标签的意义在于将每个节点归入其所属的社区，通过“传播”形成社区结构。



传播过程解析如下：

1. 标签初始化，每个节点的标签为其自身；
2. 对每个节点，用其邻居节点标签数量最多的标签更新自身的标签；
3. 执行步骤2直到节点标签变化收或者达到设定迭代次数。



任务5的输入是任务3的输出，但需要做一些预处理，至少要给每个节点打上唯一的初始标签。在算法结束之后，根据任务6的接口，还需要对输出做后处理。因此整个任务5实际上是3个MapReduce阶段。



任务3 的输出形如：

```
狄云 戚芳, 0.25 | 戚长生, 0.75
```



要给节点打上标签，初始化为其自身，并且对 value 中每个节点也加上标签，那么数据规范为：

```
狄云 狄云 | 戚芳, 0.25, 戚芳; 戚长生, 0.75, 戚长生
```



标签传播迭代次数由超参数T决定，每次循环是一个MapReduce过程。由于迭代中第t轮MapReduce将会读取第t-1轮的输出，所以保证每次迭代输入输出格式一样。在具体的标签传播过程中，map的输入为预处理的输出，map的输出有以下格式：

```
狄云 戚长生|戚芳,0.25,戚芳; 戚长生, 0.75, 戚长生;\
戚芳 狄云, 戚长生\
戚长生 狄云, 戚长生\
```



对 reduce 考虑 key 值为戚芳的节点输入若为：

```
戚芳 戚长生|狄云, 0.33, 狄云; 戚长生, 0.67, 戚长生;\
戚芳 狄云, 戚长生
```



那么其输出为：

```
戚芳 戚长生|狄云, 0.33, 戚长生; 戚长生, 0.67, 戚长生;
```



数据处理后，只保留 key 和最终的 label(value)。任务6 中希望归属同一社区，即具有相同标签的节点一起输出。利用 MapReduce 的特点可以很好的省去排序的过程，只需要将标签作为 key， 就可以将具有相同标签的节点发到同一个 reducer。

对于输入：

```
戚芳 戚长生|狄云, 0.33, 戚长生; 戚长生, 0.67, 戚长生;
```

mapper 的输出为：

```
戚长生 戚芳
```



此时 戚长生 为标签，戚芳 为 key。

reducer 只要将 key, value 互换，最终输出：

```
戚芳 戚长生
张三 李四
林玉龙 任飞燕
```



### 7.2 LabelPropagation.java

```java
/**
 * LabelPropagation.java
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Stage 5.
 */
import org.apache.hadoop.fs.FileSystem;




public class LabelPropagation {

    public static void main(String[] args) throws Exception {
        Preprocess.main();
        LPAIter.main();
        Postprocess.main();

    }
}

/**
 * Preprocess data format.
 * INPUT:	name1 [TAB] name2,0.75|name3,0.25
 * 			name2 [TAB] name1,0.75|name3,0.25
 *			name3 [TAB] name1,0.5|name2,0.5
 * OUTPUT: 	name1 [TAB] name1|name2,0.75,name2;name3,0.25,name3;
 */

class Preprocess {
    private static class PreprocessMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object off, Text text, Context context) throws IOException, InterruptedException {
            String[] parts = text.toString().split("\t");
            String key = parts[0];
            String[] neighbors = parts[1].split("\\|");

            StringBuilder outputValue = new StringBuilder(key).append("|");

            for (String neighbor : neighbors) {
                String[] neighborParts = neighbor.split(",");
                String neighborName = neighborParts[0].trim();
                String neighborWeight = neighborParts[1].trim();

                outputValue.append(neighborName).append(",").append(neighborWeight).append(",").append(neighborName).append(";");
            }

            // Remove "[" and "]" from the entire text
            String outputString = outputValue.toString().replaceAll("[\\[\\]]", "");

            context.write(new Text(key), new Text(outputString));
        }
    }

    private static class PreprocessReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values)
                context.write(key, value);
        }
    }

    public static void main() throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "LPA Preprocess ");

        job.setJarByClass(Preprocess.class);
        job.setMapperClass(PreprocessMapper.class);
        job.setReducerClass(PreprocessReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 设置输入输出路径
        FileInputFormat.addInputPath(job, new Path("file:///home/hadoop/Downloads/Normalization_output/part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path("file:///home/hadoop/Downloads/LabelPropagation_output_preprocess"));

        if (!job.waitForCompletion(true))
            throw new IllegalStateException(job.getJobName() + " failed.");
    }
}

/**
 * Iterations of Label Propagation Algorithm.
 * INPUT:	name1 [TAB] label|name2,0.75,label2;name3,0.25,label3;
 * 			name2 [TAB] labe2|name1,0.75,label1;name3,0.25,label3;
 *			name3 [TAB] labe3|name1,0.5,label1;name2,0.5,label2;
 * OUTPUT: 	name1 [TAB] newlabel1|name2,0.75,label2;name3,0.25,label3;
 *
 */

class LPAIter {
    private static String getMaxnumLabel(HashMap<String, Double> labelWeights) {
        double max = 0;
        String label = null;
        for (String key : labelWeights.keySet()) {
            if (labelWeights.get(key) > max) {
                max = labelWeights.get(key);
                label = key;
            }
        }

        return label;
    }

    /**
     * OUTPUT: 	name1 [TAB] newlabel1|name2,0.75,label2;name3,0.25,label3;
     *			name2 [TAB] name1,newlabel1
     */

    private static class LPAIterMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object off, Text text, Context context) throws IOException, InterruptedException {
            String key = text.toString().split("\t")[0];
            String value = text.toString().split("\t")[1];

            String values = value.split("\\|")[1];

            ArrayList<String> nameList = new ArrayList<>();
            HashMap<String, Double> labelWeights = new HashMap<>();

            String[] tokens = values.split(";");

            for (String str : tokens) {
                if (str.length() <= 0) continue;

                String[] tmp = str.split(",");
                if (!nameList.contains(tmp[0])) nameList.add(tmp[0]);

                double weight = Double.valueOf(tmp[1]);
                Double weightsum = labelWeights.get(tmp[2]);

                if (weightsum == null) weightsum = weight;
                else weightsum += weight;

                labelWeights.put(tmp[2], weightsum);
            }

            String newLabel = getMaxnumLabel(labelWeights);
            if (newLabel != null) {
                context.write(new Text(key), new Text(newLabel + "|" + values));
                for (String name : nameList)
                    context.write(new Text(name), new Text(key + "," + newLabel));
            }
        }
    }

    private static class LPAIterReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            String list = null;
            String label = null;

            HashMap<String, String> nameLabel = new HashMap<>();

            for (Text text : value) {
                String str = text.toString();

                if (str.contains(";")) {
                    label = str.split("\\|")[0];
                    list = str.split("\\|")[1];
                }
                else {
                    nameLabel.put(str.split(",")[0], str.split(",")[1]);
                }
            }

            if (list != null && label != null) {
                StringBuilder sb = new StringBuilder();
                sb.append(label).append("|");
                String[] tokens = list.split(";");

                for (String token : tokens) {
                    if (token.length() <= 0) continue;

                    String[] tmp = token.split(",");
                    sb.append(tmp[0]).append(",").append(tmp[1]).append(",").append(nameLabel.get(tmp[0])).append(";");
                }

                context.write(key, new Text(sb.toString()));
            }
        }
    }

    public static void main() throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "LPA Iter ");

        job.setJarByClass(LPAIter.class);
        job.setMapperClass(LPAIterMapper.class);
        job.setReducerClass(LPAIterReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("file:///home/hadoop/Downloads/LabelPropagation_output_preprocess/part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path("file:///home/hadoop/Downloads/LabelPropagation_output_LPAIter"));
        if (!job.waitForCompletion(true))
            throw new IllegalStateException(job.getJobName() + " failed.");
    }
}

/**
 * Postprocess, only names and labels remain.
 * INTPUT: 		name1 [TAB] newlabel1|name2,0.75,label2;name3,0.25,label3;
 * OUTPUT:		name1 [TAB] label1
 */
class Postprocess {

    private static class PostprocessMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object off, Text text, Context context) throws IOException, InterruptedException {
            String key = text.toString().split("\t")[0];
            String value = text.toString().split("\t")[1];

            String label = value.split("\\|")[0];
            context.write(new Text(label), new Text(key));
        }
    }

    private static class PostprocessReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text text : values)
                context.write(text, key);
        }
    }

    public static void main( ) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "LPA Postprocess ");

        job.setJarByClass(Postprocess.class);
        job.setMapperClass(PostprocessMapper.class);
        job.setReducerClass(PostprocessReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("file:///home/hadoop/Downloads/LabelPropagation_output_LPAIter/part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path("file:///home/hadoop/Downloads/LabelPropagation_output_postprocess"));

        if (!job.waitForCompletion(true)) {
            throw new IllegalStateException(job.getJobName() + " failed.");
        }
    }
}

```



### 7.3 应用程序打包，并运行程序

此目录下可以找到打包后的 .jar 文件

```
cd /home/hadoop/idea/LabelPropagation_JinYong
```



### 7.4 查看结果

程序运行后，可以在

```
/home/hadoop/Downloads
```

目录下看到三个文件，分别是

```
LabelPropagation_output_preprocess
LabelPropagation_output_LPAIter
LabelPropagation_output_postprocess
```

其中包含了预处理，LPAIter，后处理的结果

分别如下（只展示部分）：



#### Preprocess：

```
一灯大师	一灯大师|上官,0.002347417840375587,上官;丘处机,0.011737089201877934,丘处机;乔寨主,0.002347417840375587,乔寨主;农夫,0.03990610328638498,农夫;华筝,0.002347417840375587,华筝;卫璧,0.004694835680751174,卫璧;吕文德,0.002347417840375587,吕文德;周伯通,0.06572769953051644,周伯通;哑巴,0.002347417840375587,哑巴;哑梢公,0.002347417840375587,哑梢公;大汉,0.002347417840375587,大汉;天竺僧,0.002347417840375587,天竺僧;天竺僧人,0.007042253521126761,天竺僧人;完颜萍,0.004694835680751174,完颜萍;小沙弥,0.007042253521126761,小沙弥;小龙女,0.018779342723004695,小龙女;尹克西,0.002347417840375587,尹克西;尼摩星,0.007042253521126761,尼摩星;张无忌,0.004694835680751174,张无忌;无色,0.002347417840375587,无色;明月,0.002347417840375587,明月;朱九真,0.004694835680751174,朱九真;朱子柳,0.03051643192488263,朱子柳;朱长龄,0.002347417840375587,朱长龄;李莫愁,0.011737089201877934,李莫愁;杨康,0.007042253521126761,杨康;杨过,0.07981220657276995,杨过;柯镇恶,0.004694835680751174,柯镇恶;梅超风,0.002347417840375587,梅超风;樵子,0.02112676056338028,樵子;欧阳克,0.007042253521126761,欧阳克;武三娘,0.004694835680751174,武三娘;武三通,0.03286384976525822,武三通;武修文,0.007042253521126761,武修文;武敦儒,0.004694835680751174,武敦儒;武青婴,0.004694835680751174,武青婴;汉子,0.004694835680751174,汉子;洪七公,0.025821596244131457,洪七公;渔人,0.028169014084507043,渔人;潇湘子,0.002347417840375587,潇湘子;点苍渔隐,0.007042253521126761,点苍渔隐;琴儿,0.002347417840375587,琴儿;穆念慈,0.002347417840375587,穆念慈;老头子,0.002347417840375587,老头子;耶律燕,0.007042253521126761,耶律燕;耶律齐,0.011737089201877934,耶律齐;裘千丈,0.002347417840375587,裘千丈;裘千仞,0.03051643192488263,裘千仞;裘千尺,0.014084507042253521,裘千尺;觉远,0.002347417840375587,觉远;觉远大师,0.002347417840375587,觉远大师;达尔巴,0.004694835680751174,达尔巴;郝大通,0.004694835680751174,郝大通;郭芙,0.009389671361502348,郭芙;郭襄,0.04225352112676056,郭襄;郭靖,0.11737089201877934,郭靖;金轮法王,0.009389671361502348,金轮法王;陆乘风,0.002347417840375587,陆乘风;陆无双,0.01643192488262911,陆无双;霍都,0.007042253521126761,霍都;马钰,0.004694835680751174,马钰;鲁有脚,0.009389671361502348,鲁有脚;黄药师,0.035211267605633804,黄药师;黄蓉,0.16901408450704225,黄蓉;
```



#### IPAIter:

```
一灯大师	黄蓉|上官,0.002347417840375587,大汉;丘处机,0.011737089201877934,郭靖;乔寨主,0.002347417840375587,哑梢公;农夫,0.03990610328638498,黄蓉;华筝,0.002347417840375587,郭靖;卫璧,0.004694835680751174,张无忌;吕文德,0.002347417840375587,郭靖;周伯通,0.06572769953051644,郭靖;哑巴,0.002347417840375587,袁承志;哑梢公,0.002347417840375587,黄蓉;大汉,0.002347417840375587,汉子;天竺僧,0.002347417840375587,杨过;天竺僧人,0.007042253521126761,黄蓉;完颜萍,0.004694835680751174,杨过;小沙弥,0.007042253521126761,黄蓉;小龙女,0.018779342723004695,杨过;尹克西,0.002347417840375587,潇湘子;尼摩星,0.007042253521126761,潇湘子;张无忌,0.004694835680751174,赵敏;无色,0.002347417840375587,郭襄;明月,0.002347417840375587,桃红;朱九真,0.004694835680751174,张无忌;朱子柳,0.03051643192488263,杨过;朱长龄,0.002347417840375587,张无忌;李莫愁,0.011737089201877934,杨过;杨康,0.007042253521126761,黄蓉;杨过,0.07981220657276995,小龙女;柯镇恶,0.004694835680751174,郭靖;梅超风,0.002347417840375587,郭靖;樵子,0.02112676056338028,农夫;欧阳克,0.007042253521126761,黄蓉;武三娘,0.004694835680751174,李莫愁;武三通,0.03286384976525822,杨过;武修文,0.007042253521126761,杨过;武敦儒,0.004694835680751174,武修文;武青婴,0.004694835680751174,张无忌;汉子,0.004694835680751174,大汉;洪七公,0.025821596244131457,黄蓉;渔人,0.028169014084507043,黄蓉;潇湘子,0.002347417840375587,尹克西;点苍渔隐,0.007042253521126761,达尔巴;琴儿,0.002347417840375587,苗若兰;穆念慈,0.002347417840375587,黄蓉;老头子,0.002347417840375587,令狐冲;耶律燕,0.007042253521126761,完颜萍;耶律齐,0.011737089201877934,郭芙;裘千丈,0.002347417840375587,黄蓉;裘千仞,0.03051643192488263,黄蓉;裘千尺,0.014084507042253521,杨过;觉远,0.002347417840375587,郭襄;觉远大师,0.002347417840375587,张三丰;达尔巴,0.004694835680751174,杨过;郝大通,0.004694835680751174,丘处机;郭芙,0.009389671361502348,杨过;郭襄,0.04225352112676056,杨过;郭靖,0.11737089201877934,黄蓉;金轮法王,0.009389671361502348,杨过;陆乘风,0.002347417840375587,郭靖;陆无双,0.01643192488262911,杨过;霍都,0.007042253521126761,杨过;马钰,0.004694835680751174,丘处机;鲁有脚,0.009389671361502348,黄蓉;黄药师,0.035211267605633804,郭靖;黄蓉,0.16901408450704225,郭靖;
```



#### Postprocess:

```
李万山	丁不三
梅芳姑	丁不四
梅文馨	丁不四
小翠	丁不四
史小翠	丁不四
马大鸣	丁典
凌退思	丁典
陆伯	丁勉
陈大方	丁大全
慧净	丁春秋
玄难	丁春秋
薛慕华	丁春秋
范百龄	丁春秋
万震山	万圭
教书先生	万震山
鲁坤	万震山
青灵子	上官
小凤	上官
孙均	上官
```



至此任务完成！



# END
