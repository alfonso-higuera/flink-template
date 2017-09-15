package com.crabi.trip.jdbc

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.sinks.AppendStreamTableSink
import org.apache.flink.table.sinks.BatchTableSink
import org.apache.flink.table.sinks.TableSink
import org.apache.flink.types.Row
import org.apache.flink.util.InstantiationUtil
import java.io.IOException

class JDBCAppendTableSink(private val outputFormat: JDBCOutputFormat)
  : AppendStreamTableSink<Row>, BatchTableSink<Row> {

  private var fieldNames: Array<String>? = null
  private var fieldTypes: Array<TypeInformation<*>>? = null

  override fun getOutputType(): TypeInformation<Row> {
    return RowTypeInfo(fieldTypes, fieldNames)
  }

  override fun getFieldNames(): Array<String>? {
    return fieldNames
  }

  override fun getFieldTypes(): Array<TypeInformation<*>>? {
    return fieldTypes
  }

  override fun configure(
      fieldNames: Array<String>, fieldTypes: Array<TypeInformation<*>>): TableSink<Row> {
    val types: IntArray = outputFormat.typesArray
    val sinkSchema: String =
        types
            .map { JDBCTypeUtil.getTypeName(it) }
            .joinToString(separator = ", ")
    val tableSchema: String =
        fieldTypes
            .map { JDBCTypeUtil.getTypeName(it) }
            .joinToString(separator = ", ")
    val message: String =
        "Schema of output table is incompatible with JDBCAppendTableSink schema. " +
            "Table schema: [$tableSchema], sink schema: [$sinkSchema]"
    if (fieldTypes.size != types.size) {
      throw IllegalArgumentException(message)
    }
    (0 until types.size)
        .filter { JDBCTypeUtil.typeInformationToSqlType(fieldTypes[it]) != types[it] }
        .forEach { throw IllegalArgumentException(message) }

    var copy: JDBCAppendTableSink? = null
    try {
      copy = JDBCAppendTableSink(InstantiationUtil.clone(outputFormat))
    } catch (ex: Exception) {
      when (ex) {
        is IOException,
        is ClassNotFoundException ->
          throw RuntimeException(ex)
      }
    }
    copy.fieldNames = fieldNames
    copy.fieldTypes = fieldTypes
    return copy
  }

  override fun emitDataStream(dataStream: DataStream<Row>): Unit {
    dataStream.addSink(JDBCSinkFunction(outputFormat))
  }

  override fun emitDataSet(dataSet: DataSet<Row>): Unit {
    dataSet.output(outputFormat)
  }
}