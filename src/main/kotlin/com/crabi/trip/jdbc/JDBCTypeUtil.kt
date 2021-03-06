package com.crabi.trip.jdbc

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.common.typeinfo.BasicTypeInfo.BIG_DEC_TYPE_INFO
import org.apache.flink.api.common.typeinfo.BasicTypeInfo.BOOLEAN_TYPE_INFO
import org.apache.flink.api.common.typeinfo.BasicTypeInfo.BYTE_TYPE_INFO
import org.apache.flink.api.common.typeinfo.BasicTypeInfo.DOUBLE_TYPE_INFO
import org.apache.flink.api.common.typeinfo.BasicTypeInfo.FLOAT_TYPE_INFO
import org.apache.flink.api.common.typeinfo.BasicTypeInfo.INT_TYPE_INFO
import org.apache.flink.api.common.typeinfo.BasicTypeInfo.LONG_TYPE_INFO
import org.apache.flink.api.common.typeinfo.BasicTypeInfo.SHORT_TYPE_INFO
import org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo
import java.math.BigDecimal
import java.sql.Date
import java.sql.Time
import java.sql.Timestamp
import java.sql.Types

object JDBCTypeUtil {

  private val typeMapping: Map<TypeInformation<*>, Int> = mapOf(
      Pair<BasicTypeInfo<String>, Int>(STRING_TYPE_INFO, Types.VARCHAR),
      Pair<BasicTypeInfo<Boolean>, Int>(BOOLEAN_TYPE_INFO, Types.BOOLEAN),
      Pair<BasicTypeInfo<Byte>, Int>(BYTE_TYPE_INFO, Types.TINYINT),
      Pair<BasicTypeInfo<Short>, Int>(SHORT_TYPE_INFO, Types.SMALLINT),
      Pair<BasicTypeInfo<Int>, Int>(INT_TYPE_INFO, Types.INTEGER),
      Pair<BasicTypeInfo<Long>, Int>(LONG_TYPE_INFO, Types.BIGINT),
      Pair<BasicTypeInfo<Float>, Int>(FLOAT_TYPE_INFO, Types.FLOAT),
      Pair<BasicTypeInfo<Double>, Int>(DOUBLE_TYPE_INFO, Types.DOUBLE),
      Pair<SqlTimeTypeInfo<Date>, Int>(SqlTimeTypeInfo.DATE, Types.DATE),
      Pair<SqlTimeTypeInfo<Time>, Int>(SqlTimeTypeInfo.TIME, Types.TIME),
      Pair<SqlTimeTypeInfo<Timestamp>, Int>(SqlTimeTypeInfo.TIMESTAMP, Types.TIMESTAMP),
      Pair<BasicTypeInfo<BigDecimal>, Int>(BIG_DEC_TYPE_INFO, Types.DECIMAL),
      Pair<PrimitiveArrayTypeInfo<ByteArray>, Int>(BYTE_PRIMITIVE_ARRAY_TYPE_INFO, Types.BINARY)
  )

  private val typeInformationMapping: Map<Int, TypeInformation<*>> =
      typeMapping.entries.associateBy({ it.value }) { it.key }

  private val sqlTypeNames: Map<Int, String> = mapOf(
      Pair<Int, String>(Types.VARCHAR, "VARCHAR"),
      Pair<Int, String>(Types.BOOLEAN, "BOOLEAN"),
      Pair<Int, String>(Types.TINYINT, "TINYINT"),
      Pair<Int, String>(Types.SMALLINT, "SMALLINT"),
      Pair<Int, String>(Types.INTEGER, "INTEGER"),
      Pair<Int, String>(Types.BIGINT, "BIGINT"),
      Pair<Int, String>(Types.FLOAT, "FLOAT"),
      Pair<Int, String>(Types.DOUBLE, "DOUBLE"),
      Pair<Int, String>(Types.CHAR, "CHAR"),
      Pair<Int, String>(Types.DATE, "DATE"),
      Pair<Int, String>(Types.TIME, "TIME"),
      Pair<Int, String>(Types.TIMESTAMP, "TIMESTAMP"),
      Pair<Int, String>(Types.DECIMAL, "DECIMAL"),
      Pair<Int, String>(Types.BINARY, "BINARY")
  )

  fun typeInformationToSqlType(type: TypeInformation<*>): Int {
    return if (typeMapping.containsKey(type)) {
      typeMapping[type]!!
    } else if (type is ObjectArrayTypeInfo<*, *> || type is PrimitiveArrayTypeInfo) {
      Types.ARRAY
    } else {
      throw IllegalArgumentException("Unsupported type: $type")
    }
  }

  fun sqlTypeToTypeInformation(type: Int): TypeInformation<*> {
    return if (typeInformationMapping.containsKey(type)) {
      typeInformationMapping[type]!!
    } else {
      throw IllegalArgumentException("Unsupported type: $type")
    }
  }

  fun getTypeName(type: Int): String? {
    return sqlTypeNames[type]
  }

  fun getTypeName(type: TypeInformation<*>): String? {
    return sqlTypeNames[typeInformationToSqlType(type)]
  }
}
