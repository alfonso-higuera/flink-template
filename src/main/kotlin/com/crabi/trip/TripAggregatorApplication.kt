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
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.java.tuple.Tuple4
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
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.Properties

object TripAggregatorApplication {

  @JvmStatic
  fun main(args: Array<String>): Unit {
    val streamExecutionEnvironment: StreamExecutionEnvironment =
        StreamExecutionEnvironment.getExecutionEnvironment()

    val aggregateTrips = object : AggregateFunction<TripEvent, Trip, Trip> {

      override fun getResult(trip: Trip): Trip = trip

      override fun merge(firstTrip: Trip, secondTrip: Trip): Trip {
        firstTrip.gpsPoints.addAll(secondTrip.gpsPoints)
        if (firstTrip.distanceTravelledInKilometers == null &&
            secondTrip.distanceTravelledInKilometers != null) {
          firstTrip.distanceTravelledInKilometers = secondTrip.distanceTravelledInKilometers
        }
        if (secondTrip.initialTimestamp != null) {
          firstTrip.initialTimestamp =
              if (firstTrip.initialTimestamp != null) {
                minOf(firstTrip.initialTimestamp!!, secondTrip.initialTimestamp!!)
              } else {
                secondTrip.initialTimestamp
              }
        }
        if (secondTrip.finalTimestamp != null) {
          firstTrip.finalTimestamp =
              if (firstTrip.finalTimestamp != null) {
                maxOf(firstTrip.finalTimestamp!!, secondTrip.finalTimestamp!!)
              } else {
                secondTrip.finalTimestamp!!
              }
        }
        if (firstTrip.timeIdleInSeconds == null && secondTrip.timeIdleInSeconds != null) {
          firstTrip.timeIdleInSeconds = secondTrip.timeIdleInSeconds
        }
        if (firstTrip.timeMovingInSeconds == null && secondTrip.timeMovingInSeconds != null) {
          firstTrip.timeMovingInSeconds = secondTrip.timeMovingInSeconds
        }
        if (firstTrip.vehicleId == null && !secondTrip.vehicleId.isNullOrEmpty()) {
          firstTrip.vehicleId = secondTrip.vehicleId
        }
        if (firstTrip.internationalMobileEquipmentId == null &&
            !secondTrip.internationalMobileEquipmentId.isNullOrEmpty()) {
          firstTrip.internationalMobileEquipmentId = secondTrip.internationalMobileEquipmentId
        }
        return firstTrip
      }

      override fun add(tripEvent: TripEvent, trip: Trip): Unit {
        if (trip.id == null) {
          trip.id = tripEvent.id
        }
        if (trip.deviceId == null) {
          trip.deviceId = tripEvent.deviceId
        }
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
          else -> {
            if (trip.initialTimestamp == null || trip.initialTimestamp!! > tripEvent.timestamp) {
              trip.initialTimestamp = tripEvent.timestamp
            }
            if (trip.finalTimestamp == null || trip.finalTimestamp!! < tripEvent.timestamp) {
              trip.finalTimestamp = tripEvent.timestamp
            }
          }
        }
        trip.date =
            LocalDateTime
                .ofInstant(trip.initialTimestamp ?: trip.finalTimestamp, ZoneOffset.UTC)
                .withDayOfMonth(1)
                .toLocalDate()
      }

      override fun createAccumulator(): Trip =
          Trip(
              id = null,
              deviceId = null,
              date = null,
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

    val keySelector = KeySelector<TripEvent, Tuple2<Long, String>> {
      Tuple2.of(it.id, it.deviceId)
    }

    val timestampExtractor = object
      : BoundedOutOfOrdernessTimestampExtractor<TripEvent>(Time.seconds(3)) {

      override fun extractTimestamp(trip: TripEvent): Long = trip.timestamp.epochSecond
    }

    val toTripRow = MapFunction<Trip, Row> { trip: Trip ->
      val row = Row(9)
      row.setField(0, trip.id ?: -1L)
      row.setField(1, trip.deviceId ?: "")
      row.setField(2, java.sql.Date.valueOf(trip.date ?: LocalDate.MIN))
      row.setField(
          3,
          Timestamp.from(trip.initialTimestamp ?: Instant.ofEpochSecond(0))
      )
      row.setField(
          4,
          Timestamp.from(trip.finalTimestamp ?: Instant.ofEpochSecond(0))
      )
      row.setField(5, trip.distanceTravelledInKilometers ?: 0.0)
      row.setField(6, trip.timeIdleInSeconds ?: 0.0)
      row.setField(7, trip.timeMovingInSeconds ?: 0.0)
      row.setField(8, trip.internationalMobileEquipmentId ?: "")
      row
    }

    val tableEnvironment: StreamTableEnvironment =
        TableEnvironment.getTableEnvironment(streamExecutionEnvironment)

    val kafkaProperties = Properties()
    kafkaProperties.setProperty("bootstrap.servers", "10.138.0.2:9092,10.138.0.3:9092,10.138.0.4:9092")
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
                    BasicTypeInfo.STRING_TYPE_INFO,
                    SqlTimeTypeInfo.DATE,
                    SqlTimeTypeInfo.TIMESTAMP,
                    SqlTimeTypeInfo.TIMESTAMP,
                    BasicTypeInfo.DOUBLE_TYPE_INFO,
                    BasicTypeInfo.DOUBLE_TYPE_INFO,
                    BasicTypeInfo.DOUBLE_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO
                )
            )

    val tripsJdbcSink = JDBCAppendTableSink(
        outputFormat = JDBCOutputFormat(
            driverName = "org.postgresql.Driver",
            databaseUrl = "jdbc:postgresql://10.138.0.100:26257/crabi_trips?ssl=true&sslcert=/home/ahiguera/application/certs/client.trip_aggregator.crt&sslkey=/home/ahiguera/application/key/client.trip_aggregator.key.pk8&sslrootcert=/home/ahiguera/application/certs/ca.crt&sslfactory=org.postgresql.ssl.jdbc4.LibPQFactory&user=trip_aggregator&sslmode=require",
            query =
                """
                   | INSERT INTO
                   |     trip (
                   |         trip_number,
                   |         dongle_id,
                   |         trip_date,
                   |         initial_timestamp,
                   |         final_timestamp,
                   |         distance_in_kilometers,
                   |         time_idle_in_seconds,
                   |         time_moving_in_seconds,
                   |         dongle_international_mobile_equipment_id
                   |     )
                   | VALUES
                   |     (?, ?, ?, ?, ?, ?, ?, ?, ?)
                   | ON CONFLICT
                   |     (trip_number, trip_date, dongle_id)
                   | DO UPDATE SET
                   |     initial_timestamp = CASE
                   |                             WHEN trip.initial_timestamp = '1970-01-01' THEN
                   |                                 excluded.initial_timestamp
                   |                             WHEN excluded.initial_timestamp != '1970-01-01' THEN
                   |                                 least(trip.initial_timestamp, excluded.initial_timestamp)
                   |                             ELSE
                   |                                 trip.initial_timestamp
                   |                         END,
                   |     final_timestamp = CASE
                   |                           WHEN trip.final_timestamp = '1970-01-01' THEN
                   |                               excluded.final_timestamp
                   |                           WHEN excluded.final_timestamp != '1970-01-01' THEN
                   |                               greatest(trip.final_timestamp, excluded.final_timestamp)
                   |                           ELSE
                   |                               trip.final_timestamp
                   |                       END,
                   |     distance_in_kilometers = CASE
                   |                                  WHEN trip.distance_in_kilometers = 0 THEN
                   |                                      excluded.distance_in_kilometers
                   |                                  ELSE
                   |                                      trip.distance_in_kilometers
                   |                              END,
                   |     time_idle_in_seconds = CASE
                   |                                WHEN trip.time_idle_in_seconds = 0 THEN
                   |                                    excluded.time_idle_in_seconds
                   |                                ELSE
                   |                                    trip.time_idle_in_seconds
                   |                            END,
                   |     time_moving_in_seconds = CASE
                   |                                  WHEN trip.time_moving_in_seconds = 0 THEN
                   |                                      excluded.time_moving_in_seconds
                   |                                  ELSE
                   |                                      trip.time_moving_in_seconds
                   |                              END,
                   |     dongle_international_mobile_equipment_id = CASE
                   |                                                    WHEN trip.dongle_international_mobile_equipment_id = '' THEN
                   |                                                        excluded.dongle_international_mobile_equipment_id
                   |                                                    ELSE
                   |                                                        trip.dongle_international_mobile_equipment_id
                   |                                                END,
                   |     dongle_id = CASE
                   |                     WHEN trip.dongle_id = '' THEN
                   |                         excluded.dongle_id
                   |                     ELSE
                   |                         trip.dongle_id
                   |                 END,
                   |     trip_date = CASE
                   |                     WHEN trip.trip_date = NULL THEN
                   |                         excluded.trip_date
                   |                     ELSE
                   |                         trip.trip_date
                   |                 END,
                   |     trip_number = CASE
                   |                       WHEN trip.trip_number = -1 THEN
                   |                           excluded.trip_number
                   |                       ELSE
                   |                           trip.trip_number
                   |                   END
                """.trimMargin(),
            typesArray = intArrayOf(
                Types.BIGINT,
                Types.VARCHAR,
                Types.DATE,
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
        .fromDataStream(tripRows)
        .writeToSink(tripsJdbcSink)

    val toGpsPointRow = MapFunction<Tuple4<Long, String, LocalDate, GpsPoint>, Row> {
      gpsData: Tuple4<Long, String, LocalDate, GpsPoint> ->
        val row = Row(6)
        row.setField(0, Timestamp.from(gpsData.f3.timestamp))
        row.setField(1, gpsData.f3.latitude)
        row.setField(2, gpsData.f3.longitude)
        row.setField(3, gpsData.f0)
        row.setField(4, gpsData.f1)
        row.setField(5, java.sql.Date.valueOf(gpsData.f2))
        row
    }

    val gpsRows: DataStream<Row> =
        trips
            .flatMap(FlatMapFunction<Trip, Tuple4<Long, String, LocalDate, GpsPoint>> {
              trip: Trip, collector: Collector<Tuple4<Long, String, LocalDate, GpsPoint>> ->
                trip.gpsPoints.forEach {
                  collector.collect(
                      Tuple4.of(
                          trip.id,
                          trip.deviceId,
                          LocalDateTime
                              .ofInstant(it.timestamp, ZoneOffset.UTC)
                              .withDayOfMonth(1)
                              .toLocalDate(),
                          it
                      )
                  )
                }
            })
            .map(toGpsPointRow)
            .returns(
                RowTypeInfo(
                    SqlTimeTypeInfo.TIMESTAMP,
                    BasicTypeInfo.DOUBLE_TYPE_INFO,
                    BasicTypeInfo.DOUBLE_TYPE_INFO,
                    BasicTypeInfo.LONG_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO,
                    SqlTimeTypeInfo.DATE
                )
            )

    val gpsJdbcSink = JDBCAppendTableSink(
        outputFormat = JDBCOutputFormat(
            driverName = "org.postgresql.Driver",
            databaseUrl = "jdbc:postgresql://10.138.0.100:26257/crabi_trips?ssl=true&sslcert=/home/ahiguera/application/certs/client.trip_aggregator.crt&sslkey=/home/ahiguera/application/key/client.trip_aggregator.key.pk8&sslrootcert=/home/ahiguera/application/certs/ca.crt&sslfactory=org.postgresql.ssl.jdbc4.LibPQFactory&user=trip_aggregator&sslmode=require",
            query =
                """
                   | INSERT INTO
                   |     gps_data (
                   |         gps_data_timestamp,
                   |         latitude,
                   |         longitude,
                   |         trip_number,
                   |         trip_dongle_id,
                   |         trip_date
                   |     )
                   | VALUES
                   |     (?, ?, ?, ?, ?, ?)
                   | ON CONFLICT
                   |     (gps_data_timestamp, trip_number, trip_dongle_id, trip_date)
                   | DO UPDATE SET
                   |     trip_number = CASE
                   |                       WHEN gps_data.trip_number = -1 THEN
                   |                           excluded.trip_number
                   |                       ELSE
                   |                           gps_data.trip_number
                   |                   END,
                   |     latitude = excluded.latitude,
                   |     longitude = excluded.longitude
                """.trimMargin(),
            typesArray = intArrayOf(
                Types.TIMESTAMP,
                Types.DOUBLE,
                Types.DOUBLE,
                Types.BIGINT,
                Types.VARCHAR,
                Types.DATE
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
