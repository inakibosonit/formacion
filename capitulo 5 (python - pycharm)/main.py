import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import *

spark = (SparkSession.builder.appName("capitulo5").getOrCreate())

#EJERCICIO DEL CAPITULO 5





#dataframe para la tabla de empleados
jdbcDF2 = (spark
      .read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/employees")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("dbtable", "employees")
      .option("user", "root")
      .option("password", "123aA456")
      .load())

#dataframe para la tabla de titles
jdbcDF3 = (spark
      .read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/employees")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("dbtable", "titles")
      .option("user", "root")
      .option("password", "123aA456")
      .load())

#dataframe para la tabla de salaries
jdbcDF4 = (spark
      .read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/employees")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("dbtable", "salaries")
      .option("user", "root")
      .option("password", "123aA456")
      .load())

jdbcDF5 = (spark
      .read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/employees")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("dbtable", "dept_emp")
      .option("user", "root")
      .option("password", "123aA456")
      .load())

jdbcDF6 = (spark
      .read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/employees")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("dbtable", "departments")
      .option("user", "root")
      .option("password", "123aA456")
      .load())




#JOIN de tablas

jdbcDF_join = jdbcDF3.join( jdbcDF2  , jdbcDF2.emp_no  == jdbcDF3.emp_no )\
      .join(jdbcDF4 , jdbcDF4.emp_no == jdbcDF3.emp_no )\
      .join( jdbcDF5 , jdbcDF5.emp_no == jdbcDF3.emp_no )\
      .join(jdbcDF6, jdbcDF6.dept_no == jdbcDF5.dept_no )\
      .select(jdbcDF3.emp_no , jdbcDF2.birth_date, jdbcDF2.first_name,  jdbcDF2.last_name,  jdbcDF2.gender,  jdbcDF2.hire_date,  jdbcDF3.title, jdbcDF4.salary ,  jdbcDF5.from_date , jdbcDF5.to_date ,  jdbcDF6.dept_name)



jdbcDF_join.show()

window = Window.partitionBy( col("emp_no"), col("first_name"), col("last_name")).orderBy( desc( col("from_date")) )

jdbcDF_window = jdbcDF_join.select( col("emp_no"), col("first_name") , col("last_name"), col("from_date") , col("salary") ,col("title") , col("dept_name") )\
      .withColumn( "rn", row_number().over(window))\
      .where( col("rn") == lit(1) )\
      .drop( col("rn"))

jdbcDF_window.show(truncate=False)




