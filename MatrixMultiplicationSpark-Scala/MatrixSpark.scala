import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object MatrixSpark {
  def main(args: Array[ String ]) {
    val conf = new SparkConf().setAppName("MatrixSpark")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    val m_matrix = sc.textFile(args(0)).map( line => { val a = line.split(",")
                                                      (a(0).toInt,a(1).toInt,a(2).toDouble) } )
    val n_matrix = sc.textFile(args(1)).map( line => { val a = line.split(",")
                                                      (a(0).toInt,a(1).toInt,a(2).toDouble) } )
    val res = m_matrix.map( m_matrix => (m_matrix._2, m_matrix))
                    .join(n_matrix.map( n_matrix => (n_matrix._1,n_matrix)))
									  .map{ case (k, (m_matrix,n_matrix)) => ((m_matrix._1,n_matrix._2),(m_matrix._3 * n_matrix._3)) }
                    .reduceByKey((x,y) => (x+y))
                    .sortByKey(true, 0).collect()
    res.foreach(println)
    sc.stop()
  }
}