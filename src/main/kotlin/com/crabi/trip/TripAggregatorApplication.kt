package com.crabi.trip

import com.crabi.trip.jdbc.JDBCAppendTableSink
import com.crabi.trip.jdbc.JDBCOutputFormat
import com.crabi.trip.jdbc.JDBCTypeUtil
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.java.StreamTableEnvironment
import org.apache.flink.types.Row
import java.sql.Types

object TripAggregatorApplication {

  @JvmStatic
  fun main(args: Array<String>): Unit {
    val streamExecutionEnvironment: StreamExecutionEnvironment =
        StreamExecutionEnvironment.getExecutionEnvironment()

    val inputData: DataStream<String> =
        streamExecutionEnvironment
            .socketTextStream("127.0.0.1", 9000, "\n")

    val mergeBankAccountDeposits = ReduceFunction<BankAccountDeposit> {
      (firstId, firstAmount), (_, secondAmount) -> BankAccountDeposit(
        id = firstId,
        amount = firstAmount + secondAmount,
        timestamp = System.currentTimeMillis(),
        isTerminator = false
      )
    }

    val keySelector = KeySelector<BankAccountDeposit, Int> { it.id }

    val timestampExtractor = object
      : BoundedOutOfOrdernessTimestampExtractor<BankAccountDeposit>(Time.seconds(3)) {

      override fun extractTimestamp(bankAccountDeposit: BankAccountDeposit): Long {
        return bankAccountDeposit.timestamp
      }
    }

    val toRow = MapFunction<BankAccountDeposit, Row> {
      (id: Int, amount: Double, _) ->
        val row = Row(2)
        row.setField(0, id)
        row.setField(1, amount)
        row
    }

    val tableEnvironment: StreamTableEnvironment =
        TableEnvironment.getTableEnvironment(streamExecutionEnvironment)

    val rows: DataStream<Row> =
        inputData
            .map(JSONUtil.toBankAccountDeposit)
            .assignTimestampsAndWatermarks(timestampExtractor)
            .keyBy(keySelector)
            .window(GlobalWindows.create())
            .trigger(
                ProcessingTimeTrigger<BankAccountDeposit, GlobalWindow>(
                  minimumRetentionTimeInMilliseconds = Time.milliseconds(10).toMilliseconds(),
                  maximumRetentionTimeInMilliseconds = Time.seconds(5).toMilliseconds()
                )
            )
            .reduce(mergeBankAccountDeposits)
            .map(toRow)
            .returns(
                RowTypeInfo(
                    JDBCTypeUtil.sqlTypeToTypeInformation(Types.INTEGER),
                    JDBCTypeUtil.sqlTypeToTypeInformation(Types.DOUBLE)
                )
            )

    val jdbcSink = JDBCAppendTableSink(
        outputFormat = JDBCOutputFormat(
            driverName = "org.postgresql.Driver",
            databaseUrl = "jdbc:postgresql://127.0.0.1:26257/bank?user=root&sslmode=disable",
            query =
              """| INSERT INTO accounts (id, balance)
                 | VALUES (?, ?)
                 | ON CONFLICT (id) DO UPDATE SET balance = EXCLUDED.balance""".trimMargin(),
            typesArray = intArrayOf(Types.INTEGER, Types.DOUBLE)
        )
    )

    tableEnvironment
        .fromDataStream(rows)
        .writeToSink(jdbcSink)

    streamExecutionEnvironment.enableCheckpointing(Time.seconds(10).toMilliseconds())

    streamExecutionEnvironment.execute()
  }
}
