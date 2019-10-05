package course02

import org.apache.flink.api.scala.ExecutionEnvironment

/**
  **
  *
  * @author hemiao
  * @date 2019-10-04 22:31
  */
object BatchWCScalaApp {
  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val input = "F:/input.txt"

    val datasource = env.readTextFile(input)

    //引入隐式转换
    import org.apache.flink.api.scala._

    datasource.flatMap(_.toLowerCase().split("\t")).filter(_.nonEmpty)
      .map((_,1))
      .groupBy(0)
      .sum(1).print()

  }
}
