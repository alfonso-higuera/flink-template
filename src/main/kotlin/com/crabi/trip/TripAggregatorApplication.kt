package com.crabi.trip

import com.crabi.trip.models.TripEvent
import com.crabi.trip.services.TripAggregatorService
import com.crabi.trip.util.JsonUtil
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010.FlinkKafkaProducer010Configuration
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import java.util.Properties

object TripAggregatorApplication {

  @JvmStatic
  fun main(args: Array<String>): Unit {
    val streamExecutionEnvironment: StreamExecutionEnvironment =
        StreamExecutionEnvironment.getExecutionEnvironment()

    val kafkaProperties = Properties()
    kafkaProperties.setProperty("bootstrap.servers", "127.0.0.1:9092")
    kafkaProperties.setProperty("group.id", "test")
    val kafkaConsumer =
        FlinkKafkaConsumer010<String>("test", SimpleStringSchema(), kafkaProperties)
    kafkaConsumer.setStartFromLatest()

    val trips: DataStream<String> =
        streamExecutionEnvironment
            .addSource(kafkaConsumer)
            .map(JsonUtil.toTripEvent)
            .filter(TripAggregatorService.filterOutNullTripEvent)
            .map(TripAggregatorService.toNonNullableTripEvent)
            .assignTimestampsAndWatermarks(TripAggregatorService.timestampExtractor)
            .keyBy(TripAggregatorService.keySelector)
            .window(GlobalWindows.create())
            .trigger(
                ProcessingTimeTrigger<TripEvent, GlobalWindow>(
                    minimumRetentionTimeInMilliseconds = Time.milliseconds(200).toMilliseconds(),
                    maximumRetentionTimeInMilliseconds = Time.seconds(10).toMilliseconds()
                )
            )
            .aggregate(TripAggregatorService.aggregateTrips)
            .map(JsonUtil.tripToJson)
            .filter(TripAggregatorService.filterOutNullTripJson)
            .map(TripAggregatorService.toNonNullableTripJson)

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
