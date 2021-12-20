package com.josedeveloper
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
/**
 * @author ${user.name}
 */
object App {
  def main(args : Array[String]) {

    val spark:SparkSession = SparkSession.builder().master("local[1]")
      .appName("mysqlconnector")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val file = "file:///Users/inaki.abrego/Desktop/big_data/LearningSparkV2-master/avro/_/"

    val df_flights = (spark.read.format("avro").option("inferSchema", "true").load(file))

    val jdbcDF = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/mascotas")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("dbtable", "pet")
      .option("user", "root")
      .option("password", "123aA456")
      .load()

    //dataframe para la tabla de empleados
    val jdbcDF2 = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/employees")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("dbtable", "employees")
      .option("user", "root")
      .option("password", "123aA456")
      .load()

    //dataframe para la tabla de titles
    val jdbcDF3 = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/employees")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("dbtable", "titles")
      .option("user", "root")
      .option("password", "123aA456")
      .load()

    //dataframe para la tabla de salaries
    val jdbcDF4 = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/employees")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("dbtable", "salaries")
      .option("user", "root")
      .option("password", "123aA456")
      .load()

    val jdbcDF5 = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/employees")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("dbtable", "dept_emp")
      .option("user", "root")
      .option("password", "123aA456")
      .load()

    val jdbcDF6 = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/employees")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("dbtable", "departments")
      .option("user", "root")
      .option("password", "123aA456")
      .load()

     jdbcDF.show()

     df_flights.show()

     //JOIN de tabla employees con titles
     //select e.emp_no, birth_date, first_name , last_name, gender, hire_date, title, from_date, to_date from employees e inner join titles t ON e.emp_no = t.emp_no;

    val jdbcDF7 = jdbcDF3.join( jdbcDF2 as "employees" , jdbcDF2("emp_no") === jdbcDF3("emp_no")).select( col("employees.emp_no"), col("birth_date"), col("first_name") , col("last_name"), col("gender"), col("hire_date"), col("title"))

     //jdbcDF7.show()

    //JOIN  con salaries

    val jdbcDF8 = jdbcDF7.join( jdbcDF4 as "salaries" , jdbcDF4("emp_no") === jdbcDF7("emp_no")).select( col("salaries.emp_no"), col("birth_date"), col("first_name") , col("last_name"), col("gender"), col("hire_date"), col("title"), col("salary"))

    //jdbcDF8.show()

    //JOIN  con dept_emp

    val jdbcDF9 = jdbcDF8.join( jdbcDF5 as "dept_emp" , jdbcDF5("emp_no") === jdbcDF8("emp_no")).select( col("dept_emp.emp_no"), col("birth_date"), col("first_name") , col("last_name"), col("gender"), col("hire_date"), col("title"), col("salary"), col("from_date"), col("to_date"), col("dept_no"))

    //jdbcDF9.show()

    //JOIN  con departments

    val jdbcDF10 = jdbcDF9.join( jdbcDF6 as "departments" , jdbcDF6("dept_no") === jdbcDF9("dept_no")).select( col("emp_no"), col("birth_date"), col("first_name") , col("last_name"), col("gender"), col("hire_date"), col("title"), col("salary"), col("from_date"), col("to_date"), col("dept_name"))

   // val jdbcDF11 = jdbcDF10.filter(col("to_date") === lit( "9999-01-01"))

    jdbcDF10.show()


      //Utilizando operaciones de ventana obtener el salario, posición (cargo) y departamento actual de cada empleado,
      // es decir, el último o más reciente

    val window = Window.partitionBy( "emp_no", "first_name", "last_name").orderBy(desc("from_date"))

    val jdbcDF12 = jdbcDF10.select( col("emp_no"), col("first_name") , col("last_name") , col("from_date") , col("to_date") , col("title") , col("dept_name"))
      .withColumn("rn", row_number().over(window))
      .where(col("rn")===lit(1))
      .drop(col("rn"))

    jdbcDF12.show()


  }
}