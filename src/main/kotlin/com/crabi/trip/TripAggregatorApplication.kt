package com.crabi.trip

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.Properties
import java.util.TreeSet

data class PointInRadians(val x: Double, val y: Double)

object TripAggregatorApplication {

  val logger: Logger = LoggerFactory.getLogger(TripAggregatorApplication::class.java)

  private fun degreeToRadian(degree: Double): Double {
    return 2.0 * degree * Math.PI / 360.0
  }

  fun haversine(first: GpsPoint, second: GpsPoint): Double {
    val firstPointInRadians =
        PointInRadians(
            x = degreeToRadian(first.longitude),
            y = degreeToRadian(first.latitude)
        )
    val secondPointInRadians =
        PointInRadians(
            x = degreeToRadian(second.longitude),
            y = degreeToRadian(second.latitude)
        )
    val deltaPhi: Double = secondPointInRadians.x  - firstPointInRadians.x
    val deltaLambda: Double = firstPointInRadians.y - secondPointInRadians.y
    val h: Double =
        Math.pow(Math.sin(deltaPhi / 2.0), 2.0) +
            Math.cos(firstPointInRadians.x) *
                Math.cos(secondPointInRadians.x) *
                Math.pow(Math.sin(deltaLambda / 2.0), 2.0)
    val averageRadiusOfEarth = 6371.0
    return 2.0 * averageRadiusOfEarth * Math.asin(Math.sqrt(h))
  }

  @JvmStatic
  fun main(args: Array<String>): Unit {
    val streamExecutionEnvironment: StreamExecutionEnvironment =
        StreamExecutionEnvironment.getExecutionEnvironment()

    val aggregateTrips = object : AggregateFunction<Trip, TripAggregation, TripAggregation> {

      override fun merge(first: TripAggregation, second: TripAggregation): TripAggregation {
        first.gpsPoints.addAll(second.gpsPoints)
        first.timestamps.addAll(second.timestamps)
        return first
      }

      override fun add(trip: Trip, tripAggregation: TripAggregation) {
        logger.warn("$trip $tripAggregation")
        when (trip) {
          is TripData -> {
            tripAggregation.timestamps.add(trip.timestamp)
            val gpsPoints: List<GpsPointAggregate> =
                trip
                    .parameterIdsData
                    .filter { it is GpsData }
                    .map { it as GpsData }
                    .map { GpsPointAggregate(timestamp = trip.timestamp, gpsPoint = it.gpsPoint) }
            tripAggregation
                .gpsPoints
                .addAll(gpsPoints)
            val vehicleSpeeds: List<VehicleSpeedAggregate> =
                trip
                    .parameterIdsData
                    .filter { it is VehicleSpeed }
                    .map {
                      VehicleSpeedAggregate(
                          timestamp = trip.timestamp,
                          vehicleSpeed = it as VehicleSpeed
                      )
                    }
            tripAggregation.speeds.addAll(vehicleSpeeds)
          }
          is TripStart -> {
            tripAggregation.timestamps.add(trip.timestamp)
            tripAggregation.vehicleId = trip.vehicleId
          }
        }
      }

      private fun timeDeltas(speeds: Collection<VehicleSpeedAggregate>): Collection<Long> {
        return speeds
            .drop(1)
            .zip(speeds)
            .map { (first: VehicleSpeedAggregate, second: VehicleSpeedAggregate) ->
              second.timestamp.epochSecond - first.timestamp.epochSecond
            }
      }

      private fun calculateTotalTimeDurationInSeconds(timestamps: Collection<Instant>): Long {
        if (timestamps.isEmpty()) {
          return 0L
        }
        val maximumInstant: Instant? = timestamps.max()
        val minimumInstant: Instant? = timestamps.min()
        if (maximumInstant == null || minimumInstant == null) {
          return -1L
        }
        return maximumInstant.epochSecond - minimumInstant.epochSecond
      }

      override fun getResult(tripAggregation: TripAggregation): TripAggregation {
        tripAggregation.distanceInMeter =
            tripAggregation
                .gpsPoints
                .drop(1)
                .zip(tripAggregation.gpsPoints)
                .map { (x: GpsPointAggregate, y: GpsPointAggregate) -> haversine(x.gpsPoint, y.gpsPoint) }
                .sum()
        val consecutiveLowSpeedSequences = ArrayList<Collection<VehicleSpeedAggregate>>()
        var speeds: Collection<VehicleSpeedAggregate> = tripAggregation.speeds
        while (speeds.isNotEmpty()) {
          val consecutiveLowSpeedSequence: List<VehicleSpeedAggregate> =
             speeds.takeWhile { it.vehicleSpeed.speedInKilometerPerHour < 5 }
          speeds = if (consecutiveLowSpeedSequence.isNotEmpty()) {
            consecutiveLowSpeedSequences.add(consecutiveLowSpeedSequence)
            speeds.drop(consecutiveLowSpeedSequence.size)
          } else {
            speeds.drop(1)
          }
        }
        tripAggregation.timeDurationStoppedInSeconds =
            consecutiveLowSpeedSequences.flatMap { timeDeltas(it) }.sum()
        tripAggregation.totalTimeDurationInSeconds = calculateTotalTimeDurationInSeconds(
            timestamps =
              tripAggregation.gpsPoints.map { it.timestamp } + tripAggregation.speeds.map { it.timestamp }
        )
        tripAggregation.timeDurationMovingInSeconds =
            tripAggregation.totalTimeDurationInSeconds -
                tripAggregation.timeDurationStoppedInSeconds
        return tripAggregation
      }

      override fun createAccumulator(): TripAggregation {
        return TripAggregation(
            timestamps = TreeSet<Instant>(),
            gpsPoints = TreeSet<GpsPointAggregate>(GpsPointAggregate.timestampComparator),
            speeds = TreeSet<VehicleSpeedAggregate>(VehicleSpeedAggregate.timestampComparator),
            totalTimeDurationInSeconds = 0L,
            timeDurationMovingInSeconds = 0L,
            timeDurationStoppedInSeconds = 0L,
            vehicleId = "",
            distanceInMeter = 0.0
        )
      }
    }

    val keySelector = KeySelector<Trip, Long> { it.id }

    val timestampExtractor = object
      : BoundedOutOfOrdernessTimestampExtractor<Trip>(Time.seconds(3)) {

      override fun extractTimestamp(trip: Trip): Long {
        return trip.timestamp.epochSecond
      }
    }

    /*val toRow = MapFunction<BankAccountDeposit, Row> {
      (id: Int, amount: Double, _, isTerminator: Boolean) ->
        val row = Row(3)
        row.setField(0, id)
        row.setField(1, amount)
        row.setField(2, isTerminator)
        row
    }

    val tableEnvironment: StreamTableEnvironment =
        TableEnvironment.getTableEnvironment(streamExecutionEnvironment)*/

    val kafkaProperties = Properties()
    kafkaProperties.setProperty("bootstrap.servers", "localhost:9092")
    kafkaProperties.setProperty("group.id", "test")
    val kafkaConsumer =
        FlinkKafkaConsumer010<String>("testTopic", SimpleStringSchema(), kafkaProperties)

    val filterFn = FilterFunction<Trip?> { it != null }

    val toBankFn = MapFunction<Trip?, Trip> { it as Trip }

    val rows: DataStream<TripAggregation> =
        streamExecutionEnvironment
            .addSource(kafkaConsumer)
            .map(JSONUtil.toTrip)
            .filter(filterFn)
            .map(toBankFn)
            .assignTimestampsAndWatermarks(timestampExtractor)
            .keyBy(keySelector)
            .window(GlobalWindows.create())
            .trigger(
                ProcessingTimeTrigger<Trip, GlobalWindow>(
                    minimumRetentionTimeInMilliseconds = Time.milliseconds(10).toMilliseconds(),
                    maximumRetentionTimeInMilliseconds = Time.seconds(4).toMilliseconds()
                )
            )
            .aggregate(aggregateTrips)
            /*.map(toRow)
            .returns(
                RowTypeInfo(
                    JDBCTypeUtil.sqlTypeToTypeInformation(Types.INTEGER),
                    JDBCTypeUtil.sqlTypeToTypeInformation(Types.DOUBLE),
                    JDBCTypeUtil.sqlTypeToTypeInformation(Types.BOOLEAN)
                )
            )

    val jdbcSink = JDBCAppendTableSink(
        outputFormat = JDBCOutputFormat(
            driverName = "org.postgresql.Driver",
            databaseUrl = "jdbc:postgresql://127.0.0.1:26257/bank?user=root&sslmode=disable",
            query =
                """| INSERT INTO accounts (id, balance, is_terminator)
                   | VALUES (?, ?, ?)
                   | ON CONFLICT (id) DO UPDATE SET
                   |     balance = accounts.balance + excluded.balance,
                   |     is_terminator = accounts.is_terminator OR excluded.is_terminator
                """.trimMargin(),
            typesArray = intArrayOf(Types.INTEGER, Types.DOUBLE, Types.BOOLEAN)
        )
    )

    tableEnvironment
        .fromDataStream(rows)
        .writeToSink(jdbcSink)*/

    rows.print()

    streamExecutionEnvironment.enableCheckpointing(Time.seconds(5).toMilliseconds())

    streamExecutionEnvironment.execute()
  }
}
