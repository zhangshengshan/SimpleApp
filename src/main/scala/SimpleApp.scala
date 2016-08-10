import org.apache.spark.{SparkConf, SparkContext}
object SimpleApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    hiveContext.sql("use bi")
    val seckill_df = hiveContext.sql("select * from seckill_stat")
    val distinct_kdt = seckill_df.select("kdt_id").distinct
    distinct_kdt.collect().foreach(println)
    val distinct_fm_title = seckill_df.select("fm","title").distinct
    distinct_fm_title.filter(!(distinct_fm_title("fm")==="display" or distinct_fm_title("fm")==="preorder")).collect().foreach(println)
  }
}

