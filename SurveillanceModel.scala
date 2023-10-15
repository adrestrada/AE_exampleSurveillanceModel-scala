package test1

object SurveillanceModel {
  def main(args: Array[String]): Unit = {
    // Simulación/ejemplo de datos de transacciones financieras
    val transactions = List(
      Transaction(1, "2023-01-01", 1000, "Compra", "cliente Adri"),
      Transaction(2, "2023-01-02", 2500, "Retiro", "cliente Berto"),
      Transaction(3, "2023-01-03", 800, "Compra", "cliente Alba"),
      Transaction(4, "2023-01-04", 3000, "Retiro", "cliente Carlos"),
      Transaction(5, "2023-01-05", 1500, "Compra", "cliente Barragan")
    )
    // Identificar transacciones inusuales
    val unusualTransactions = transactions.filter(_.monto > 1200) //--->  se conoce como una función lambda o función anónima.
    //( _.) (recorre cada elemento)

    // Registrar transacciones inusuales
    unusualTransactions.foreach { transaction =>
      println(s"Transacción inusual detectada: " +
        s"ID ${transaction.id}, " +
        s"Cliente ${transaction.cliente}, " +
        s"Monto ${transaction.monto}")
    }
  }
}
case class Transaction(id: Int, fecha: String, monto: Int, tipo: String, cliente: String) //---> nombre de las columnas
// en orden


//-----> Podemos agregar datos de una fuente externa usando spark y sus funciones
// Apache Spark para cargar y procesar los datos de transacciones desde una fuente externa
// (en este caso, un archivo CSV). Luego, aplicamos una serie de transformaciones a los datos
// utilizando Spark SQL para identificar y seleccionar las transacciones con montos superiores a 2000.


//import org.apache.spark.sql.{SparkSession, DataFrame}
//import org.apache.spark.sql.functions._
//
//object SurveillanceModel {
//  def main(args: Array[String]): Unit = {
//    val spark = SparkSession.builder()
//      .appName("SurveillanceModel")
//      .getOrCreate()
//
//    // Cargar datos de transacciones desde una fuente externa (por ejemplo, un archivo CSV)
//    val transactions: DataFrame = spark.read
//      .option("header", "true")
//      .csv("ruta/al/archivo.csv")
//
//    // Definir una función para identificar transacciones inusuales
//    val unusualTransactions = transactions
//      .filter(col("monto") > 2000)
//      .select("ID de Transacción", "Cliente", "Monto")
//
//    // Mostrar las transacciones inusuales
//    unusualTransactions.show()
//
//    // Detener la sesión Spark
//    spark.stop()
//  }
//}