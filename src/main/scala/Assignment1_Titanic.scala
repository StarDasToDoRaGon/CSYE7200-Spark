import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object Assignment1_Titanic extends App {
  // 创建一个SparkSession
  val spark = SparkSession.builder()
    .appName("CSV Reader")
    .master("local")
    .getOrCreate()

  // 读取CSV文件
  val df = spark.read
    .option("header", "true") // 使用第一行作为标题
    .option("inferSchema", "true") // 推断数据类型（注意：这可能会稍微减慢读取过程）
    .csv("E:/CSYE7200Assignments/train.csv") // 替换为你的文件路径

  // 显示DataFrame的前几行以确认读取成功
  //df.show()
  // 1)What is the average ticket fare for each Ticket class?
  df.groupBy("Pclass").agg(avg("Fare")).show()

  // 2) What is the survival percentage for each Ticket class? Which class has the highest survival rate?
  df.groupBy("pclass")
    .agg((sum("survived") / count("survived")).alias("survival_rate"))
    .orderBy(desc("survival_rate"))
    .show()

  // 3) find the number of passengers who could possibly be Rose
  println(df.filter(
    col("Sex") === "female" &&
      col("age") === 17 &&
      col("pclass") === 1 &&
      col("parch") === 1 &&
      col("SibSp") === 0 &&
      col("parch") === 1).count())

  // 4)  Find the number of passengers who could possibly be Jack? ( PS: Yeah he's the guy who gets Rose )
  println(df.filter(
    (col("age") === 20 || col("age") === 19) &&
      col("pclass") === 3 &&
      col("sibsp") === 0 &&
      col("sex") === "male"
  ).count())

  // 5)  What is the relation between the ages and the ticket fare? Which age group most likely survived ?
  //首先，创建一个新的列来表示年龄组：
  val dfWithAgeGroup = df.withColumn("age_group", (col("age") / 10).cast("integer") * 10)

  //然后，计算每个年龄组的生存率：
  dfWithAgeGroup.groupBy("age_group")
    .agg((sum("survived") / count("survived")).alias("survival_rate"))
    .orderBy(desc("survival_rate"))
    .show()

  //最后，找出年龄和票价之间的关系：
  dfWithAgeGroup.groupBy("age_group")
    .agg(avg("fare").alias("average_fare"))
    .orderBy("age_group")
    .show()



  // 关闭SparkSession
  spark.stop()
}
