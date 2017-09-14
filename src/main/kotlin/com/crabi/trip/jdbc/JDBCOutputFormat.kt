package com.crabi.trip.jdbc

import org.apache.flink.api.common.io.RichOutputFormat
import org.apache.flink.configuration.Configuration
import org.apache.flink.types.Row
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.SQLException

class JDBCOutputFormat(
    val userName: String? = null,
    val password: String? = null,
    val driverName: String,
    val databaseUrl: String,
    val query: String,
    val typesArray: IntArray
) : RichOutputFormat<Row>() {

  private val serialVersionUID: Long = -1L

  private val logger: Logger = LoggerFactory.getLogger(JDBCOutputFormat::class.java)
  private var batchInterval: Int = 5000

  private var databaseConnection: Connection? = null
  private var uploadStatement: PreparedStatement? = null
  var isDatabaseConnectionOpen: Boolean = false

  private var batchCount: Int = 0

  override fun writeRecord(row: Row): Unit {
    if (typesArray.isNotEmpty() && typesArray.size != row.arity) {
      throw IllegalArgumentException(
          "Column SQL types array doesn't match arity of passed Row! Check the passed array...")
    }

    val upload: PreparedStatement =
        uploadStatement ?: throw NullPointerException("The JDBC prepared statement is not initialized")

    try {
      for (i in 0..row.arity) {
        if (row.getField(i) == null) {
          upload.setNull(i + 1, typesArray[i])
        } else {
          when (typesArray[i]) {
            java.sql.Types.NULL ->
              upload.setNull(i + 1, typesArray[i])
            java.sql.Types.BOOLEAN,
            java.sql.Types.BIT ->
              upload.setBoolean(i + 1, row.getField(i) as Boolean)
            java.sql.Types.CHAR,
            java.sql.Types.NCHAR,
            java.sql.Types.VARCHAR,
            java.sql.Types.LONGVARCHAR,
            java.sql.Types.LONGNVARCHAR ->
              upload.setString(i + 1, row.getField(i) as String)
            java.sql.Types.TINYINT ->
              upload.setByte(i + 1, row.getField(i) as Byte)
            java.sql.Types.SMALLINT ->
              upload.setShort(i + 1, row.getField(i) as Short)
            java.sql.Types.INTEGER ->
              upload.setInt(i + 1, row.getField(i) as Int)
            java.sql.Types.BIGINT ->
              upload.setLong(i + 1, row.getField(i) as Long)
            java.sql.Types.REAL ->
              upload.setFloat(i + 1, row.getField(i) as Float)
            java.sql.Types.FLOAT,
            java.sql.Types.DOUBLE ->
              upload.setDouble(i + 1, row.getField(i) as Double)
            java.sql.Types.DECIMAL,
            java.sql.Types.NUMERIC ->
              upload.setBigDecimal(i + 1, row.getField(i) as java.math.BigDecimal)
            java.sql.Types.DATE ->
              upload.setDate(i + 1, row.getField(i) as java.sql.Date)
            java.sql.Types.TIME ->
              upload.setTime(i + 1, row.getField(i) as java.sql.Time)
            java.sql.Types.TIMESTAMP ->
              upload.setTimestamp(i + 1, row.getField(i) as java.sql.Timestamp)
            java.sql.Types.BINARY,
            java.sql.Types.VARBINARY,
            java.sql.Types.LONGVARBINARY ->
              upload.setBytes(i + 1, row.getField(i) as ByteArray)
            else -> {
              upload.setObject(i + 1, row.getField(i))
              logger.warn(
                  "Un managed sql type (${typesArray[i]}) for column ${i + 1}. Best effort approach to set its value: ${row.getField(i)}.")
            }
          }
        }
      }
      upload.addBatch()
      ++batchCount
    } catch (ex: SQLException) {
      throw RuntimeException("Preparation of JDBC statement failed.", ex)
    }

    if (batchCount >= batchInterval) {
      flush()
    }
  }

  override fun configure(configuration: Configuration): Unit { }

  override fun close(): Unit {
    if (isDatabaseConnectionOpen) {
      if (uploadStatement != null) {
        flush()
        try {
          uploadStatement!!.close()
        } catch (ex: SQLException) {
          logger.info("JDBC statement could not be closed: ${ex.message}")
        } finally {
          uploadStatement = null
        }
      }

      if (databaseConnection != null) {
        try {
          databaseConnection!!.close()
        } catch (ex: SQLException) {
          logger.info("JDBC connection could not be closed: ${ex.message}")
        } finally {
          databaseConnection = null
        }
      }

      isDatabaseConnectionOpen = false
    }
  }

  fun flush(): Unit {
    if (isDatabaseConnectionOpen) {
      try {
        if (uploadStatement != null) {
          uploadStatement!!.executeBatch()
        }
        batchCount = 0
      } catch (ex: SQLException) {
        throw RuntimeException("Execution of JDBC statement failed.", ex)
      }
    }
  }

  override fun open(taskNumber: Int, numberOfTask: Int): Unit {
    try {
      establishConnection()
      uploadStatement = databaseConnection!!.prepareStatement(query)
      isDatabaseConnectionOpen = true
    } catch (ex: SQLException) {
      throw IllegalArgumentException("open() failed.", ex)
    } catch (ex: ClassNotFoundException) {
      throw IllegalArgumentException("JDBC driver class not found.", ex)
    }
  }

  private fun establishConnection(): Unit {
    Class.forName(driverName)
    databaseConnection =
        if (userName == null) {
          DriverManager.getConnection(databaseUrl)
        } else {
          DriverManager.getConnection(databaseUrl, userName, password)
        }
  }
}