package com.crabi.trip

import com.crabi.trip.jdbc.JDBCAppendTableSink
import com.crabi.trip.jdbc.JDBCOutputFormat
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.java.StreamTableEnvironment
import org.apache.flink.types.Row
import java.sql.Timestamp
import java.sql.Types
import java.time.Instant
import java.util.Properties
import java.util.TreeSet

object TripAggregatorApplication {

  @JvmStatic
  fun main(args: Array<String>): Unit {
    val streamExecutionEnvironment: StreamExecutionEnvironment =
        StreamExecutionEnvironment.getExecutionEnvironment()

    val aggregateTrips = object : AggregateFunction<Trip, TripAggregation, TripAggregation> {

      override fun merge(first: TripAggregation, second: TripAggregation): TripAggregation {
        first.gpsPoints.addAll(second.gpsPoints)
        first.timestamps.addAll(second.timestamps)
        first.timestamps.addAll(second.timestamps)
        first.speeds.addAll(second.speeds)
        first.totalTimeDurationInSeconds += second.totalTimeDurationInSeconds
        first.timeDurationMovingInSeconds += second.timeDurationMovingInSeconds
        first.timeDurationStoppedInSeconds += second.timeDurationStoppedInSeconds
        if (second.vehicleId.isNotEmpty()) {
          first.vehicleId = second.vehicleId
        }
        return first
      }

      override fun add(trip: Trip, tripAggregation: TripAggregation) {
        tripAggregation.id = trip.id
        tripAggregation.timestamps.add(trip.timestamp)
        when (trip) {
          is TripData -> {
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
          is TripStart ->
            tripAggregation.vehicleId = trip.vehicleId
          is TripEvent -> {
            if (trip.eventData is OnBoardDiagnosticsSpeedEvent &&
                trip.eventData.data is TripSpeedMetrics) {
              tripAggregation.distanceInKilometers += trip.eventData.data.distanceInKilometers
              tripAggregation.totalTimeDurationInSeconds += trip.eventData.data.timeDurationInSeconds
            }
          }
        }
      }

      private fun timeDeltas(speeds: Collection<VehicleSpeedAggregate>): Collection<Long> {
        return speeds
            .drop(1)
            .zip(speeds)
            .map { (first: VehicleSpeedAggregate, second: VehicleSpeedAggregate) ->
              first.timestamp.epochSecond - second.timestamp.epochSecond
            }
      }

      override fun getResult(tripAggregation: TripAggregation): TripAggregation {
        val consecutiveLowSpeedSequences = ArrayList<Collection<VehicleSpeedAggregate>>()
        var speeds: Collection<VehicleSpeedAggregate> = tripAggregation.speeds
        while (speeds.isNotEmpty()) {
          val consecutiveLowSpeedSequence: List<VehicleSpeedAggregate> =
             speeds.takeWhile { it.vehicleSpeed.speedInKilometerPerHour < 2 }
          speeds = if (consecutiveLowSpeedSequence.isNotEmpty()) {
            consecutiveLowSpeedSequences.add(consecutiveLowSpeedSequence)
            speeds.drop(consecutiveLowSpeedSequence.size)
          } else {
            speeds.drop(1)
          }
        }
        tripAggregation.timeDurationStoppedInSeconds =
            consecutiveLowSpeedSequences.flatMap { timeDeltas(it) }.sum().toDouble()
        tripAggregation.timeDurationMovingInSeconds =
            tripAggregation.totalTimeDurationInSeconds -
                tripAggregation.timeDurationStoppedInSeconds
        return tripAggregation
      }

      override fun createAccumulator(): TripAggregation {
        return TripAggregation(
            id = -1L,
            timestamps = TreeSet<Instant>(),
            gpsPoints = TreeSet<GpsPointAggregate>(GpsPointAggregate.timestampComparator),
            speeds = TreeSet<VehicleSpeedAggregate>(VehicleSpeedAggregate.timestampComparator),
            totalTimeDurationInSeconds = 0.0,
            timeDurationMovingInSeconds = 0.0,
            timeDurationStoppedInSeconds = 0.0,
            vehicleId = "",
            distanceInKilometers = 0.0
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

    val toRow = MapFunction<TripAggregation, Row> { tripAggregation: TripAggregation ->
      val row = Row(7)
      row.setField(0, tripAggregation.id)
      row.setField(1, Timestamp.from(tripAggregation.timestamps.first()))
      row.setField(2, Timestamp.from(tripAggregation.timestamps.last()))
      row.setField(3, tripAggregation.distanceInKilometers)
      row.setField(4, tripAggregation.timeDurationStoppedInSeconds)
      row.setField(5, tripAggregation.timeDurationMovingInSeconds)
      row.setField(6, tripAggregation.vehicleId)
      row
    }

    val tableEnvironment: StreamTableEnvironment =
        TableEnvironment.getTableEnvironment(streamExecutionEnvironment)

    val kafkaProperties = Properties()
    kafkaProperties.setProperty("bootstrap.servers", "10.138.0.2:9092,10.138.0.3:9092,10.138.0.4:9092")
    kafkaProperties.setProperty("group.id", "test")
    val kafkaConsumer =
        FlinkKafkaConsumer010<String>("test", SimpleStringSchema(), kafkaProperties)
    kafkaConsumer.setStartFromEarliest()

    val filterNulls = FilterFunction<Trip?> { it != null }

    val toNonNullable = MapFunction<Trip?, Trip> { it as Trip }

    val tripAggregations: DataStream<TripAggregation> =
        streamExecutionEnvironment
            .addSource(kafkaConsumer)
            .map(JSONUtil.toTrip)
            .filter(filterNulls)
            .map(toNonNullable)
            .assignTimestampsAndWatermarks(timestampExtractor)
            .keyBy(keySelector)
            .window(GlobalWindows.create())
            .trigger(
                ProcessingTimeTrigger<Trip, GlobalWindow>(
                    minimumRetentionTimeInMilliseconds = Time.milliseconds(200).toMilliseconds(),
                    maximumRetentionTimeInMilliseconds = Time.seconds(10).toMilliseconds()
                )
            )
            .aggregate(aggregateTrips)

    val rows: DataStream<Row> =
        tripAggregations
          .map(toRow)
          .returns(
              RowTypeInfo(
                  BasicTypeInfo.LONG_TYPE_INFO,
                  SqlTimeTypeInfo.TIMESTAMP,
                  SqlTimeTypeInfo.TIMESTAMP,
                  BasicTypeInfo.DOUBLE_TYPE_INFO,
                  BasicTypeInfo.DOUBLE_TYPE_INFO,
                  BasicTypeInfo.DOUBLE_TYPE_INFO,
                  BasicTypeInfo.STRING_TYPE_INFO
              )
          )

    val jdbcSink = JDBCAppendTableSink(
        outputFormat = JDBCOutputFormat(
            driverName = "org.postgresql.Driver",
            databaseUrl = "jdbc:postgresql://10.138.0.100:26257/trips?ssl=true&sslcert=/home/ahiguera/application/certs/client.trip_aggregator.crt&sslkey=/home/ahiguera/application/key/client.trip_aggregator.key.pk8&sslrootcert=/home/ahiguera/application/certs/ca.crt&sslfactory=org.postgresql.ssl.jdbc4.LibPQFactory&user=trip_aggregator&sslmode=require",
            query =
                """| INSERT INTO trip (
                   |    bitbrew_trip_id,
                   |    initial_timestamp,
                   |    final_timestamp,
                   |    distance_in_kilometers,
                   |    time_duration_stopped_in_seconds,
                   |    time_duration_moving_in_seconds,
                   |    vehicle_id
                   | )
                   | VALUES (?, ?, ?, ?, ?, ?, ?)
                   | ON CONFLICT (bitbrew_trip_id, vehicle_id) DO UPDATE SET
                   |     initial_timestamp = LEAST(trip.initial_timestamp, excluded.initial_timestamp),
                   |     final_timestamp = GREATEST(trip.final_timestamp, excluded.final_timestamp),
                   |     distance_in_kilometers = trip.distance_in_kilometers + excluded.distance_in_kilometers,
                   |     time_duration_stopped_in_seconds = trip.time_duration_stopped_in_seconds + excluded.time_duration_stopped_in_seconds,
                   |     time_duration_moving_in_seconds = trip.time_duration_moving_in_seconds + excluded.time_duration_moving_in_seconds,
                   |     vehicle_id = CASE
                   |                      WHEN trip.vehicle_id = '' THEN
                   |                          excluded.vehicle_id
                   |                      ELSE
                   |                          trip.vehicle_id
                   |                  END,
                   |     bitbrew_trip_id = CASE
                   |                           WHEN trip.bitbrew_trip_id = -1 THEN
                   |                               excluded.bitbrew_trip_id
                   |                           ELSE
                   |                               trip.bitbrew_trip_id
                   |                       END
                """.trimMargin(),
            typesArray = intArrayOf(
                Types.BIGINT,
                Types.TIMESTAMP,
                Types.TIMESTAMP,
                Types.DOUBLE,
                Types.DOUBLE,
                Types.DOUBLE,
                Types.VARCHAR
            )
        )
    )

    tableEnvironment
        .fromDataStream(rows)
        .writeToSink(jdbcSink)

    streamExecutionEnvironment.enableCheckpointing(Time.seconds(30).toMilliseconds())

    streamExecutionEnvironment.execute()
  }
}
