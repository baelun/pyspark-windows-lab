Pyspark
==
## 1. 什麼是 PySpark ？
- PySpark 是 Apache Spark 提供的 Python API，讓開發者能用 Python 撰寫分散式資料處理程式。
- 支援 RDD（彈性分散式資料集）、DataFrame 與 Dataset，提供高階抽象與效能優化功能。
- 適合用於大數據 ETL、資料分析、機器學習等場景。

### 主要特點：
- **Structured Streaming**：支援微批次（micro-batch）和連續處理（experimental）。
- **豐富的 API**：支援 SQL、DataFrame、機器學習（MLlib）、圖計算（GraphX）。
- **生態系**：與 Hadoop、Kafka、ClickHouse 等整合良好。


**注意**：
- 與 Spark 原生的差異：Spark原是基於Scala語言的工具，性能表現較佳，生態系統也較為成熟。
- 叢集管理（例如 YARN）增加運維負擔。

## 2. 為何需要 PySpark？
當資料規模成長到無法在單機有效處理時（如數百 GB 到 TB 等等），傳統的 Python 工具（如 pandas）會面臨以下瓶頸：
1. 資料量太大，無法放入記憶體
2. 單機運算速度慢，無法並行
3. 需要整合多種資料來源與格式
4. 需要 ETL + 分析 + 機器學習一條龍

而pyspark正提供了以下功能：
1. 將資料分散到多台機器上處理（分散式運算），可處理數 TB 等級的大數據。
2. 自動處理並行與分散運算，能夠加速資料處理效率。
3. 提供統一的 API 可以讀寫這些來源，簡化整合流程。
4. 可建構端到端的資料處理流程。


## 3. PySpark 架構說明
- **Driver Program**：負責執行主邏輯，傳送任務到 Executors。
- **Executor**：在工作節點上執行實際計算任務。
- **Cluster Manager**：負責資源分配（如 Spark Standalone、YARN、Mesos）。
- PySpark 程式碼在 Driver 中定義，轉換為 DAG 後再由 Spark 分派到叢集執行。


## 4. 使用方式
### 4.1 安裝與開發環境建置
1. 下載並安裝Java JDK
    - [Java Downloads | Oracle](https://www.oracle.com/java/technologies/javase/jdk17-archive-downloads.html) 中下載安裝JDK 找尋合適JDK版本 (目前選用17)
    
2. 下載安裝 spark-hadoop 合適版本 (目前是3.5.5)
    - [Spark release](https://spark.apache.org/downloads.html) 下載 3.5.5 版本之 .tgz 檔並解壓縮
4. 下載 winutils.exe 在 D:\applications\hadoop\bin 中 (可自行選擇放置路徑)。
    - 在 [hadoop-3.3.5](https://github.com/cdarlint/winutils/tree/master/hadoop-3.3.5/bin)/bin 中下載 winutils.exe 放至某個放置路徑
    - Spark 使用 Hadoop 的檔案系統 API來管理檔案，而 Hadoop 在 Windows 上需要 winutils.exe 支援原生操作。
5. 將 JAVA_HOME、SPARK_HOME 和 Hadoop 環境變數設置為各套件安裝目錄並添加到系統的 PATH 中。
```typescript
@echo off
echo Setting up permanent environment variables for Spark...
:: 設置JAVA_HOME
setx JAVA_HOME "C:\Program Files\Java\jdk-17" /M

:: 設置 SPARK_HOME
setx SPARK_HOME "D:\applications\spark-3.5.5-bin-hadoop3" /M

:: 設置 HADOOP_HOME
setx HADOOP_HOME "D:\applications\hadoop" /M

:: 設置 PYTHONPATH
setx PYTHONPATH "D:\applications\spark-3.5.5-bin-hadoop3\python\lib\pyspark.zip;D:\applications\spark-3.5.5-bin-hadoop3\python\lib\py4j-0.10.9.7.zip;%PYTHONPATH%" /M

:: 更新 PATH
setx PATH "%JAVA_HOME%\bin;%SPARK_HOME%\bin;%HADOOP_HOME%\bin;%PATH%" /M

echo Environment variables set. Please close and reopen CMD to apply changes.
pause
```
- 將以上存成完整 CMD 腳本（一次性執行）setup_spark_env.bat → 「以系統管理員身份執行」。

5. 安裝pyspark (python版本需為3.11)
    - `pip install pyspark`
    - 通過在終端中運行 pyspark 來驗證安裝。這將啟動 PySpark shell。

6. 運行基本的 PySpark script.py或啟動 PySpark shell 以確認一切正常。


安裝步驟可參考：https://www.getorchestra.io/guides/how-to-install-java-for-pyspark-a-step-by-step-guide

### 4.2 安裝注意事項
以上為基本spark環境架設，後續實際所需之 source 與 destination 的 spark 連線 jar 套件請參考各平台官方網站提供之說明，同時須注意相容的套件版本。

**Known issue：**
- win執行的暫存刪除error (可先忽略) https://issues.apache.org/jira/browse/SPARK-12216

### 4.2 基本操作
1. 創建與設定 PysparkSession
    ```python
    # 將會需要用到的連線connector先定義在這(使用maven)
    packages = [
            "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1",
            "com.clickhouse:clickhouse-jdbc:0.6.3",
        ]

    try:
        # jars_list = get_jar_files(config['jars_dir'])
        spark = SparkSession.builder \
            .appName(config['spark']['app_name']) \
            .config("spark.local.dir", config['spark']['temp_dir']) \
            .config("spark.jars.packages", ",".join(packages)) \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        logger.info("SparkSession created successfully")
        return spark
    except Exception as e:
        logger.error(f"Failed to create SparkSession: {str(e)}")
        raise

    ```

2. 建立 DataFrame
    - 使用RDD
    ```python
    rdd = spark.sparkContext.parallelize([1, 2, 3])
    ```
    - 利用Python的DataFrame
    ```python
    df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
    ```
    - 利用CSV/Json
    ```python
    df = spark.read.option("header", "true")\
        .option("inferSchema", "true")\
        .option("delimiter", ",")\
        .csv("path.csv")
    ```
    - 使用DB Data (通常會使用此方式進行ETL，以mongoDB為例)
    ```python
    df = spark.read \
                .format("mongo") \
                .option("uri","輸入mongoDB URI") \
                .load()
    ```
    - 若是使用自動下載jar的寫法，下載預設位置為`<C:\Users\.ivy2\jars>`






2. 轉換與動作（Transformations & Actions）
    - 常見操作：`select()、filter()、groupBy()、withColumn() `等
    - UDF（使用者自定義函式）

3. Sink Data
    - 寫成檔案形式
        ```python
        df.write.csv('foo.csv', header=True)
        spark.read.csv('foo.csv', header=True).show()
        df.write.parquet('bar.parquet')
        spark.read.parquet('bar.parquet').show()
        df.write.orc('zoo.orc')
        spark.read.orc('zoo.orc').show()
        ```
    - 寫到 DBMS
        - 不同的Destination有不同的設定方式，需參考各平台的文件進行撰寫。
        - 以Clickhouse為例：
        ```python
        try:
            processed_df.write \
                .format("jdbc") \
                .option("url","請輸入url") \
                .option("dbtable", "請輸入table_name") \
                .option("driver", "請輸入driver") \
                .option("user", "請輸入User") \
                .option("password", "請輸入password") \
                .mode('append') \  # 寫入方式
                .save()
            logger.info("Data successfully written to ClickHouse")
        except Exception as e:
            logger.error(f"Error writing to ClickHouse: {str(e)}")
            raise
        ```

## 注意事項
- 常見錯誤：
    - Py4JJavaError
    - 錯誤的 schema 轉換或欄位不存在
    - 連接器設定錯誤（如 JDBC URL）

- 除錯方式：
    - 檢查 Spark UI（預設 port 4040）
    - 使用 .explain() 檢視執行計畫
    - 使用 log 驗證 dataframe schema 與 sample 資料
    
- 暫存檔案無法自動刪除：
    - pyspark在運行結束時若跳出無法刪暫存檔案，可先設置其他的`spark.local.dir`位置方便管理未刪除檔案。
- 相容性：
    - 每一個 connector在使用時須注意spark與jdbc的版本相容性問題。
- 優化Spark：
    - 可達到目的的程式碼有多種設置，但不正確的配置和低效的編碼實踐可能會導致執行緩慢和資源消耗過高。
    - [參考spark-optimization](https://soutir.substack.com/p/spark-optimizations-and-interview)
## 執行概念
### 一、Spark-Session 是什麼？
SparkSession 是 Spark 程式的切入點，所有 Spark 程式都應以創建 SparkSession 實例開始。 使用 SparkSession.builder（） 創建 SparkSession。包括以下上下文接口：
- Spark Context  Spark 上下文
- SQL Context  SQL 上下文
- Streaming Context  流式上下文
- Hive Context  Hive 上下文

```python
val spark = SparkSession.builder()
  .master("local[1]")
  .appName("SparkExamples")
  .config("spark.sql.execution.arrow.enabled", "true")
  .getOrCreate();
```
- **master**：設置運行方式。 local 代表本機運行，local[4] 代表在本機分配 4 核運行。 如果要在集群上運行，通常它可能是 yarn 或 mesos， 這取決於你的集群配置
- **appName**：設置 spark 應用程式的名字，可以在 web UI 介面看到
- **config**：額外配置項
- **getOrCreate**：如果已經存在，則返回 SparkSession 物件; 如果不存在，則創建新物件。

### 二、Spark-context 是什麼？
是 Spark 應用程式的切入點，它定義在 org.apache.Spark 包中，用來在集群上創建 RDD、累加器、廣播變數。
每個 JVM 裡只能存在一個處於 active 狀態的 SparkContext，在創建新的 SparkContext 之前必須調用 stop（） 來關閉之前的 SparkContext
每一個 Spark 應用都是一個 SparkContext 實例，可以理解為一個 SparkContext 就是一個 Spark 應用的生命週期， 一旦 SparkContext 創建之後，就可以用這個 SparkContext 來創建 RDD、累加器、廣播變數。

### 三、RDD 是什麼？
RDD（彈性分散式資料集） 是 Spark 的核心抽象，代表的是一個不可變、可並行操作的分散式資料集合。
#### 特性：
- 容錯性（Resilient）： 可透過 lineage 自動重新運算遺失資料。
- 分散式（Distributed）： 數據被拆分儲存在不同節點上。
- 不可變（Immutable）： 每次操作會產生新的 RDD，不會改變原來的資料。
- 惰性求值（Lazy Evaluation）： 僅在 action 被呼叫時才執行。

#### RDD 是如何並行運行的？
Spark 背後的運作方式是：
- 把 RDD 拆成多個 partition。
- 將每個 partition 的計算任務分派給 Executor 中的 task。
- 每個 Executor 可以執行多個 Task（可能在多個 thread 上並行運作，依 JVM 與 executor core 數設定而定）。
- 所以實際執行計算的是 JVM thread( 由spark自行控制 )，但你在 PySpark 層不會直接操控。

### 四、Spark Job？誰決定何時產生？
Job 的產生，是由 Spark 根據「觸發操作（Action）」來自動決定的。
```python
rdd = sc.textFile("data.txt")
filtered = rdd.filter(lambda x: "error" in x)  # 這不會產生 Job（lazy）
count = filtered.count()  # ✅ 這裡才觸發 Job
```
- filter 是 transformation（轉換），不會馬上執行。
- count() 是 action，會觸發一個 Job。
- Spark 會自動把從 textFile() 到 count() 所有操作串成 DAG，然後送出一個 Job。
:::info
實際會觸發 Job 的 RDD Action 類型：.start() .collect() 更多請看[spark-rdd-actions](https://spark-examples.readthedocs.io/en/latest/spark-rdd/spark-rdd-actions.html)

範例說明可參考：https://medium.com/@john_tringham/spark-concepts-simplified-lazy-evaluation-d398891e0568
:::
### 五、多個 Job 是怎麼分配的？
> 提交多個 Job 給 Spark（例如連續執行兩個 .collect()跟一個.save）
local模式
1. 把每個 Job 分成多個 Stage
2. 每個 Stage 再分成 Task（通常等於 partition 數量）
3. Spark 的 DAGScheduler 會排程 Job
4. TaskScheduler 再將 Task 分配給空閒的 Executor 來執行
:::warning
- Job 是由 Spark Driver 控制
- Driver組裝DAG，觸發Job
- Spark的排程系統（DAGScheduler, TaskScheduler）控制這些 Job 的執行順序與資源分配。
:::

<img width="1485" height="450" alt="image" src="https://github.com/user-attachments/assets/ea0aad7d-9f57-4aa4-b432-4afcb82ac247" />


### 六、Apache Spark 架構的關鍵元件是什麼？
Apache Spark 採用 主從式（Master–Worker）架構，主要包含以下元件：
| 元件                     | 說明                                                                     |
| ---------------------- | ---------------------------------------------------------------------- |
| **Driver Program**     | 執行應用程式的主控制邏輯，負責建立 SparkContext、轉換邏輯與提交任務。                              |
| **Cluster Manager**    | 管理叢集資源（如 YARN、Mesos、Kubernetes、Standalone）。                            |
| **Executor**           | 在工作節點上執行任務（Tasks），負責實際的計算與資料儲存。                                        |
| **Task**               | 最小的執行單位，由 Driver 指派給 Executors 執行。                                     |
| **Job / Stage / Task** | Spark 執行的層級結構：<br>Job → Stage（根據 Shuffle 邊界切分）→ Task（每個 Partition 一個）。 |


### 七、Spark 如何處理記憶體管理以及最佳實踐是什麼？
| 區域                   | 用途                                           |
| -------------------- | -------------------------------------------- |
| **Execution Memory** | 用於執行中計算（如 shuffle、join、aggregation）。         |
| **Storage Memory**   | 用於快取（cache）、儲存中間資料（如 broadcast、RDD persist）。 |

最佳實踐：
- 調整 Spark Memory Fraction：
    - spark.memory.fraction（預設 0.6）控制 execution + storage 總量。
- 避免過度快取：
    - 只 cache 會重複使用的資料集。
- 使用 DataFrame API：
    - Catalyst optimizer 能自動優化記憶體使用。
- 啟用 Tungsten 引擎：
    - 使用 off-heap 儲存與二進位格式減少 GC。
- 合理設定並行度（spark.sql.shuffle.partitions）：
    - 避免過多小分區導致額外開銷。

### 八、Spark 中的隨機操作與優化
隨機操作（Randomized Operations）指使用 randomSplit()、sample() 或隨機亂數生成的動作。這些操作常受分區與種子設定影響。

最佳化建議：
- 設定固定 random seed → 確保結果可重現。
- 降低重複 sample 的次數 → 可 cache 已抽樣的資料。
- 若要生成隨機數列，可使用 rand()、randn() 的分散式函數（避免 driver 生成）。

### 九、Spark 記憶體不足（OOM）錯誤的常見原因與解法
| 原因                     | 說明                                 | 解法                                               |
| ---------------------- | ---------------------------------- | ------------------------------------------------ |
| **資料過大無法放入 Executor**  | 分區過少，單一 Task 載入太多資料                | 調整 `spark.sql.shuffle.partitions`、增加分區           |
| **過度快取資料**             | 使用 `cache()` 太多且無釋放                | 僅快取重用資料，手動 `unpersist()`                         |
| **Driver 收集過多資料**      | `collect()` 或 `toPandas()` 導致記憶體爆掉 | 使用 `take()` 或寫入檔案代替                              |
| **不合理 Join / Shuffle** | 大表與大表直接 join                       | 使用 Broadcast Join 或 repartition                  |
| **GC 過多**              | JVM 堆太小                            | 調整 `--executor-memory` 或 `spark.memory.fraction` |




## Streaming Job
### Spark 如何處理 Streaming Job？
| 概念 | 說明 |
| ---| --- |
| 每個 `.writeStream.start()` | 會產生一個 **獨立的 Spark Job**（在 JVM 裡執行） |
| Spark 會自動分配資源 | 基於 Driver/Executor 模型，動態排程 |
| 預設為懶執行 (lazy evaluation) | Transformation 直到 action 才真正執行 |
| 可控的是「Spark 使用幾個執行緒」 | 用 `.master("local[*]")`、`executor.cores`(多主機) 限制 |

這行才會觸發：
```
query1 = df1.writeStream.format("console").start()
```
- 建立 Query（相當於一個 Streaming job）
- 後台會有 scheduler 去處理每個 micro-batch（或 continuous processing）
- Spark 開始分配資源（core、executor）來處理這個流

### 🔍 如果對接kafka需要多個topic，並想要分離topic處理呢?
```
df1 = spark.readStream.format("kafka").option("subscribe", "topicA").load()
df2 = spark.readStream.format("kafka").option("subscribe", "topicB").load()

query1 = df1.writeStream.format("console").start()
query2 = df2.writeStream.format("console").start()
```
每個query都會：
- 啟動一組獨立的DAG+Job
- 在 Spark 裡各自執行自己的讀取、轉換與寫入流程
- 共用同一個 SparkSession & JVM，但每個 query 各自維護執行狀態
:::warning
⚠️ 注意事項
1. 你設定的 .master() 決定最多能用幾個 CPU 執行緒：
`.master("local[4]")  # 最多使用 4 個執行緒處理 task`
2. Spark 自己負責排程各個 query 的 task，讓它們公平地分享 executor thread
3. 兩個 query 不會互相干擾，但會競爭資源（像是 memory、CPU）
:::
:::danger
* 每個 query 都會佔用 Driver thread、JVM thread、resource pool。
* 如果每個 topic 資料量都大，會導致：
    * **記憶體爆掉**
    * **GC 頻繁**
    * **CPU 滿載**
    * **延遲變大**

* 因為會有資源爭奪問題，所以可以考慮合併topic或是限制 maxConcurrentQureies。
:::

### 資源設定示例說明
#### 執行緒設置

| 項目 | `.master("local[4]")` | `spark.executor.cores` |
| ---| ---| --- |
| 用途 | 控制 **本機**可用 core 數 | 控制每個 executor 可用 core 數 |
| 使用情境 | Local 模式（開發測試） | Cluster 模式（部署上線） |
| 跑在哪裡 | 你電腦的 JVM | Executor（叢集中的 JVM） |
| 配置位置 | 在程式裡 `.master()` | 在 spark-submit 的參數或 SparkConf |

```plain
SparkSession.builder.master("local[4]").getOrCreate()
```
> → Spark 會在你電腦上啟動一個 JVM，用 4 條 thread 來並行執行 task。

### 🛠 Structured Streaming 實作紀錄

#### 1. 問題描述：WriteStream 在 `memory` & `console` format 遇到 Windows IO 錯誤  
:::danger
terminated with exception: 'boolean org.apache.hadoop.io.nativeio.NativeIO$Windows.access0(java.lang.String, int)'
:::

> **解法**：  
> 下載 `hadoop.dll`，並放到 Hadoop 安裝目錄（`HADOOP_HOME/bin` 或相似位置），即可正常運行。



#### 2. Schema Inference 注意事項
> Spark 預設不自動推斷 streaming 資料來源 schema，必須手動設定：  
> ```python
> spark.conf.set("spark.sql.streaming.schemaInference", "true")
> ```
:::warning
1. **隨機推斷**：啟動時 Spark 可能從資料目錄中隨機選一個檔案推斷 schema，若該檔案尚未寫完或格式不完整，job 會直接掛掉。  
2. **Checkpoint 相容性**：schema 會存進 checkpoint metadata，若 schema 改動，可能導致 job 無法恢復。  
3. **效能成本**：每次啟動都要讀檔並推斷 schema，對 JSON/CSV 格式耗時較重。
:::

#### 5. Checkpoint 行為

> **觀察**：當 job 已經跑到第 5 個 batch、關閉程式後，中間再往 Kafka 寫入新資料，重新啟動 PySpark 程式  
> – Spark 會**自動繼續**從最新 checkpoint offset 處理新批次資料，與 `startingOffsets` 設成 `"earliest"` 或 `"latest"` 無關。

:::info
在有 checkpoint 的情況下，`startingOffsets` 不再影響讀取位置，Spark 永遠從 checkpoint 記錄的 offset 繼續執行。
:::

#### 參考資源
- **oyspark連線sasl範例**：  Dinesh Kumar’s Medium 教學  
  https://medium.com/@dineshkumarkct/introduction-to-structured-streaming-in-apache-spark-pyspark-kafka-9151b01d79ba

- **WriteStream 模式說明**：  
  Spark 官方文件  
  https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
