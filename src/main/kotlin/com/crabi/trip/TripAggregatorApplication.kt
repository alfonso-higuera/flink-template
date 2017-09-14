package com.crabi.trip

import com.crabi.trip.jdbc.JDBCAppendTableSink
import com.crabi.trip.jdbc.JDBCOutputFormat
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.Window
import org.apache.flink.table.sinks.TableSink
import org.apache.flink.types.Row
import java.sql.Types

object TripAggregatorApplication {

  @JvmStatic
  fun main(args: Array<String>): Unit {
    val streamExecutionEnvironment: StreamExecutionEnvironment =
        StreamExecutionEnvironment.getExecutionEnvironment()

    val inputData: DataStream<String> = streamExecutionEnvironment.fromElements(
        "a:3:1461756862001:false",
        "a:4:1461756862002:true",
        "a:1:1461756862000:false",
        "b:3:1461756862001:false",
        "b:4:1461756862102:true",
        "b:1:1461756862000:false"
    )

    val atFirstTerminatorSeen: FirstElementWithPropertyTrigger<MyData, Window> =
        FirstElementWithPropertyTrigger.of { it.isTerminator }

    val toMyData = MapFunction<String, MyData> { MyData.of(it) }

    val mergeData = ReduceFunction<MyData> {
      first, second -> MyData(
        key = first.key,
        value = first.value + second.value,
        timestamp = System.currentTimeMillis(),
        isTerminator = false
      )
    }

    val keySelector = KeySelector<MyData, String> { it.key }

    val timestampExtractor = object : AscendingTimestampExtractor<MyData>() {
      override fun extractAscendingTimestamp(myData: MyData): Long {
        return myData.timestamp
      }
    }

    val sink: TableSink<Row> = JDBCAppendTableSink(
        outputFormat = JDBCOutputFormat(
            userName = "root",
            driverName = "org.postgresql.Driver",
            databaseUrl = "jdbc:postgresql://root@127.0.0.1:26257?sslmode=disable",
            query = "INSERT INTO bank.accounts VALUES (?, ?)",
            typesArray = intArrayOf(Types.VARCHAR, Types.VARCHAR)
        )
    )

    val aggregation: DataStream<MyData> =
        inputData
            .map(toMyData)
            .assignTimestampsAndWatermarks(timestampExtractor)
            .keyBy(keySelector)
            .window(EventTimeSessionWindows.withGap(Time.milliseconds(500)))
            .trigger(atFirstTerminatorSeen)
            .reduce(mergeData)

    aggregation.print()

    streamExecutionEnvironment.execute()
  }
}
