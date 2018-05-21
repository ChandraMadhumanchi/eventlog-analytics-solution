package com.user.spark

object DataFrameImprovements {
  implicit class DataFrameImprovements(df: org.apache.spark.sql.DataFrame) {
    def hasColumn(colName: String) = df.columns.contains(colName)
}
}