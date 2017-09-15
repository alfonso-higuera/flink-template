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
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.Window
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.java.StreamTableEnvironment
import org.apache.flink.types.Row
import java.sql.Types

object TripAggregatorApplication {

  @JvmStatic
  fun main(args: Array<String>): Unit {
    val streamExecutionEnvironment: StreamExecutionEnvironment =
        StreamExecutionEnvironment.getExecutionEnvironment()

    val inputData: DataStream<String> = streamExecutionEnvironment.fromElements(
        "1:3.32:1461756862001:false",
        "1:1.2:1461756862000:false",
        "1:4.32:1461756862002:true",
        "2:3.0:1461756862001:false",
        "2:4.32:1461756862102:true",
        "2:1.32:1461756862000:false",
        "3:1.232:1461756862001:true"
    )

    val terminatorTrigger: FirstElementWithPropertyTrigger<BankAccountDeposit, Window> =
        FirstElementWithPropertyTrigger.of { it.isTerminator }

    val toBankAccountDeposit = MapFunction<String, BankAccountDeposit> {
      BankAccountDeposit.of(it)
    }

    val mergeBankAccountDeposits = ReduceFunction<BankAccountDeposit> {
      (firstId, firstAmount), (_, secondAmount) -> BankAccountDeposit(
        id = firstId,
        amount = firstAmount + secondAmount,
        timestamp = System.currentTimeMillis(),
        isTerminator = false
      )
    }

    val keySelector = KeySelector<BankAccountDeposit, Int> { it.id }

    val timestampExtractor = object : AscendingTimestampExtractor<BankAccountDeposit>() {

      override fun extractAscendingTimestamp(bankAccountDeposit: BankAccountDeposit): Long {
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

    val rows: DataStream<Row> =
        inputData
            .map(toBankAccountDeposit)
            .assignTimestampsAndWatermarks(timestampExtractor)
            .keyBy(keySelector)
            .window(EventTimeSessionWindows.withGap(Time.milliseconds(500)))
            .trigger(terminatorTrigger)
            .reduce(mergeBankAccountDeposits)
            .map(toRow)
            .returns(
                RowTypeInfo(
                    JDBCTypeUtil.sqlTypeToTypeInformatin(Types.INTEGER),
                    JDBCTypeUtil.sqlTypeToTypeInformatin(Types.DOUBLE)
                )
            )

    rows.print()

    val tableEnvironment: StreamTableEnvironment =
        TableEnvironment.getTableEnvironment(streamExecutionEnvironment)

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

    tableEnvironment.fromDataStream(rows).writeToSink(jdbcSink)

    streamExecutionEnvironment.execute()
  }
}
