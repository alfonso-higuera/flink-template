package com.crabi.trip

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010.FlinkKafkaProducer010Configuration
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
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

    val kafkaProperties = Properties()
    kafkaProperties.setProperty("bootstrap.servers", "127.0.0.1:9092")
    kafkaProperties.setProperty("group.id", "test")
    val kafkaConsumer =
        FlinkKafkaConsumer010<String>("test", SimpleStringSchema(), kafkaProperties)
    kafkaConsumer.setStartFromLatest()

    val filterOutNullTripEvent = FilterFunction<TripEvent?> { it != null }
    val filterOutNullTripJson = FilterFunction<String?> { it != null }

    val toNonNullableTripEvent = MapFunction<TripEvent?, TripEvent> { it!! }
    val toNonNullableTripJson = MapFunction<String?, String> { it!! }

    val trips: DataStream<String> =
        streamExecutionEnvironment
            .addSource(kafkaConsumer)
            .map(JsonUtil.toTripEvent)
            .filter(filterOutNullTripEvent)
            .map(toNonNullableTripEvent)
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
            .map(JsonUtil.tripToJson)
            .filter(filterOutNullTripJson)
            .map(toNonNullableTripJson)

    val kafkaProducerConfiguration: FlinkKafkaProducer010Configuration<String> =
        FlinkKafkaProducer010.writeToKafkaWithTimestamps(
            trips,
            "my-topic",
            SimpleStringSchema(),
            kafkaProperties
        )
    // The following is necessary for at-least-once delivery guarantee.
    kafkaProducerConfiguration.setLogFailuresOnly(false)
    kafkaProducerConfiguration.setFlushOnCheckpoint(true)

    streamExecutionEnvironment.enableCheckpointing(Time.seconds(30).toMilliseconds())

    streamExecutionEnvironment.execute()
  }
}
