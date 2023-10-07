import java.io.FileWriter
import scala.util.Random

object FileWriter {
  def main(args: Array[String]): Unit = {
    while (true) {
      new Runnable {
        override def run(): Unit = {
          val writer = new FileWriter(s"C:\\Users\\sathi\\working\\filestream\\inputfile${System.currentTimeMillis()}.csv")
          writer.write(s"Random-Number:${Random.nextInt(1000)}")
          writer.close()
        }
        Thread.sleep(5000)
      }.run()
    }
  }

}
