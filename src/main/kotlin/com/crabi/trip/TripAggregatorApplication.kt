package com.crabi.trip

import com.crabi.trip.jdbc.JDBCAppendTableSink
import com.crabi.trip.jdbc.JDBCOutputFormat
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.tuple.Tuple3
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
import org.apache.flink.util.Collector
import java.sql.Timestamp
import java.sql.Types
import java.time.Instant
import java.util.Properties

object TripAggregatorApplication {

  @JvmStatic
  fun main(args: Array<String>): Unit {
    val streamExecutionEnvironment: StreamExecutionEnvironment =
        StreamExecutionEnvironment.getExecutionEnvironment()

    val aggregateTrips = object : AggregateFunction<TripEvent, Trip, Trip> {

      override fun getResult(trip: Trip): Trip {
        return trip
      }

      override fun merge(firstTrip: Trip, secondTrip: Trip): Trip {
        firstTrip.gpsPoints.addAll(secondTrip.gpsPoints)
        if (secondTrip.distanceTravelledInKilometers != null) {
          firstTrip.distanceTravelledInKilometers = secondTrip.distanceTravelledInKilometers
        }
        if (secondTrip.initialTimestamp != null) {
          firstTrip.initialTimestamp = secondTrip.initialTimestamp
        }
        if (secondTrip.finalTimestamp != null) {
          firstTrip.finalTimestamp = secondTrip.finalTimestamp
        }
        if (secondTrip.timeIdleInSeconds != null) {
          firstTrip.timeIdleInSeconds = secondTrip.timeIdleInSeconds
        }
        if (secondTrip.timeMovingInSeconds != null) {
          firstTrip.timeMovingInSeconds = secondTrip.timeMovingInSeconds
        }
        if (secondTrip.vehicleId != null) {
          firstTrip.vehicleId = secondTrip.vehicleId
        }
        if (secondTrip.internationalMobileEquipmentId != null) {
          firstTrip.internationalMobileEquipmentId = secondTrip.internationalMobileEquipmentId
        }
        return firstTrip
      }

      override fun add(tripEvent: TripEvent, trip: Trip): Unit {
        trip.id = tripEvent.id
        trip.gpsPoints.add(
            GpsPoint(
                timestamp = tripEvent.timestamp,
                latitude = tripEvent.latitude,
                longitude = tripEvent.longitude
            )
        )
        when (tripEvent) {
          is TripStart -> {
            trip.initialTimestamp = tripEvent.timestamp
            trip.vehicleId = tripEvent.vehicleId
            trip.internationalMobileEquipmentId = tripEvent.internationalMobileEquipmentId
          }
          is TripEnd -> {
            trip.finalTimestamp = tripEvent.timestamp
            trip.distanceTravelledInKilometers = tripEvent.distanceTravelledInKilometers
            trip.timeIdleInSeconds = tripEvent.timeIdleInSeconds
            trip.timeMovingInSeconds = tripEvent.timeMovingInSeconds
          }
        }
      }

      override fun createAccumulator(): Trip {
        return Trip(
            id = null,
            initialTimestamp = null,
            finalTimestamp = null,
            distanceTravelledInKilometers = null,
            timeMovingInSeconds = null,
            timeIdleInSeconds = null,
            vehicleId = null,
            internationalMobileEquipmentId = null,
            gpsPoints = ArrayList<GpsPoint>()
        )
      }
    }

    val keySelector = KeySelector<TripEvent, Long> { it.id }

    val timestampExtractor = object
      : BoundedOutOfOrdernessTimestampExtractor<TripEvent>(Time.seconds(3)) {

      override fun extractTimestamp(trip: TripEvent): Long {
        return trip.timestamp.epochSecond
      }
    }

    val toTripRow = MapFunction<Trip, Row> { trip: Trip ->
      val row = Row(8)
      row.setField(0, trip.id ?: -1L)
      row.setField(
          1,
          if (trip.initialTimestamp != null) {
            Timestamp.from(trip.initialTimestamp)
          } else {
            Timestamp.from(Instant.ofEpochMilli(0L))
          }
      )
      row.setField(
          2,
          if (trip.finalTimestamp != null) {
            Timestamp.from(trip.finalTimestamp)
          } else {
            Timestamp.from(Instant.ofEpochMilli(0L))
          }
      )
      row.setField(3, trip.distanceTravelledInKilometers ?: 0.0)
      row.setField(4, trip.timeIdleInSeconds ?: 0.0)
      row.setField(5, trip.timeMovingInSeconds ?: 0.0)
      row.setField(6, trip.internationalMobileEquipmentId ?: "")
      row.setField(7, trip.vehicleId ?: "")
      row
    }

    val tableEnvironment: StreamTableEnvironment =
        TableEnvironment.getTableEnvironment(streamExecutionEnvironment)

    val kafkaProperties = Properties()
    kafkaProperties.setProperty("bootstrap.servers", "127.0.0.1:9092")
    kafkaProperties.setProperty("group.id", "test")
    val kafkaConsumer =
        FlinkKafkaConsumer010<String>("test", SimpleStringSchema(), kafkaProperties)
    kafkaConsumer.setStartFromLatest()

    val filterNulls = FilterFunction<TripEvent?> { it != null }

    val toNonNullable = MapFunction<TripEvent?, TripEvent> { it!! }

    val trips: DataStream<Trip> =
        streamExecutionEnvironment
            .addSource(kafkaConsumer)
            .map(JsonUtil.toTripEvent)
            .filter(filterNulls)
            .map(toNonNullable)
            .assignTimestampsAndWatermarks(timestampExtractor)
            .keyBy(keySelector)
            .window(GlobalWindows.create())
            .trigger(
                ProcessingTimeTrigger<TripEvent, GlobalWindow>(
                    minimumRetentionTimeInMilliseconds = Time.milliseconds(200).toMilliseconds(),
                    maximumRetentionTimeInMilliseconds = Time.seconds(10).toMilliseconds()
                )
            )
            .aggregate(aggregateTrips)

    trips.print()

    val tripRows: DataStream<Row> =
        trips
            .map(toTripRow)
            .returns(
                RowTypeInfo(
                    BasicTypeInfo.LONG_TYPE_INFO,
                    SqlTimeTypeInfo.TIMESTAMP,
                    SqlTimeTypeInfo.TIMESTAMP,
                    BasicTypeInfo.DOUBLE_TYPE_INFO,
                    BasicTypeInfo.DOUBLE_TYPE_INFO,
                    BasicTypeInfo.DOUBLE_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO
                )
            )

    val tripsJdbcSink = JDBCAppendTableSink(
        outputFormat = JDBCOutputFormat(
            driverName = "org.postgresql.Driver",
            databaseUrl = "jdbc:postgresql://127.0.0.1:26257/trips?user=root&ssl=false&sslmode=disable",
            query =
                """| INSERT INTO trip (
                   |    id,
                   |    initial_timestamp,
                   |    final_timestamp,
                   |    distance_in_kilometers,
                   |    time_idle_in_seconds,
                   |    time_moving_in_seconds,
                   |    international_mobile_equipment_id,
                   |    vehicle_id
                   | )
                   | VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                   | ON CONFLICT (id, international_mobile_equipment_id) DO UPDATE SET
                   |     initial_timestamp = LEAST(trip.initial_timestamp, excluded.initial_timestamp),
                   |     final_timestamp = GREATEST(trip.final_timestamp, excluded.final_timestamp),
                   |     distance_in_kilometers = trip.distance_in_kilometers + excluded.distance_in_kilometers,
                   |     time_idle_in_seconds = trip.time_idle_in_seconds + excluded.time_idle_in_seconds,
                   |     time_moving_in_seconds = trip.time_moving_in_seconds + excluded.time_moving_in_seconds,
                   |     vehicle_id = CASE
                   |                      WHEN trip.vehicle_id = '' THEN
                   |                          excluded.vehicle_id
                   |                      ELSE
                   |                          trip.vehicle_id
                   |                  END,
                   |     international_mobile_equipment_id = CASE
                   |                                             WHEN trip.international_mobile_equipment_id = '' THEN
                   |                                                 excluded.international_mobile_equipment_id
                   |                                             ELSE
                   |                                                 trip.international_mobile_equipment_id
                   |                                         END,
                   |     id = CASE
                   |              WHEN trip.id = -1 THEN
                   |                  excluded.id
                   |              ELSE
                   |                  trip.id
                   |          END
                """.trimMargin(),
            typesArray = intArrayOf(
                Types.BIGINT,
                Types.TIMESTAMP,
                Types.TIMESTAMP,
                Types.DOUBLE,
                Types.DOUBLE,
                Types.DOUBLE,
                Types.VARCHAR,
                Types.VARCHAR
            )
        )
    )

    tableEnvironment
        .fromDataStream(tripRows)
        .writeToSink(tripsJdbcSink)

    val toGpsPointRow = MapFunction<Tuple3<Long, String, GpsPoint>, Row> {
      gpsData: Tuple3<Long, String, GpsPoint> ->
        val row = Row(5)
        row.setField(0, Timestamp.from(gpsData.f2.timestamp))
        row.setField(1, gpsData.f2.latitude)
        row.setField(2, gpsData.f2.longitude)
        row.setField(3, gpsData.f0)
        row.setField(4, gpsData.f1)
        row
    }

    val gpsRows: DataStream<Row> =
        trips
            .flatMap(FlatMapFunction<Trip, Tuple3<Long, String, GpsPoint>> {
              trip: Trip, collector: Collector<Tuple3<Long, String, GpsPoint>> ->
                trip.gpsPoints.forEach {
                  collector.collect(Tuple3.of(trip.id, trip.internationalMobileEquipmentId, it))
                }
            })
            .map(toGpsPointRow)
            .returns(
                RowTypeInfo(
                    SqlTimeTypeInfo.TIMESTAMP,
                    BasicTypeInfo.DOUBLE_TYPE_INFO,
                    BasicTypeInfo.DOUBLE_TYPE_INFO,
                    BasicTypeInfo.LONG_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO
                )
            )

    val gpsJdbcSink = JDBCAppendTableSink(
        outputFormat = JDBCOutputFormat(
            driverName = "org.postgresql.Driver",
            databaseUrl = "jdbc:postgresql://127.0.0.1:26257/trips?user=root&ssl=false&sslmode=disable",
            query =
                """| INSERT INTO gps_data (
                   |    gps_data_timestamp,
                   |    latitude,
                   |    longitude,
                   |    trip_id,
                   |    trip_international_mobile_equipment_id
                   | )
                   | VALUES (?, ?, ?, ?, ?)
                   | ON CONFLICT (gps_data_timestamp, trip_id, trip_international_mobile_equipment_id) DO NOTHING
                """.trimMargin(),
            typesArray = intArrayOf(
                Types.TIMESTAMP,
                Types.DOUBLE,
                Types.DOUBLE,
                Types.BIGINT,
                Types.VARCHAR
            )
        )
    )

    tableEnvironment
        .fromDataStream(gpsRows)
        .writeToSink(gpsJdbcSink)

    streamExecutionEnvironment.enableCheckpointing(Time.seconds(30).toMilliseconds())

    streamExecutionEnvironment.execute()
  }
}
