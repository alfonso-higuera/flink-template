package com.crabi.trip

import com.google.gson.Gson
import org.apache.flink.api.common.functions.MapFunction
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Instant
import java.time.ZonedDateTime

object JsonUtil {

  private val logger: Logger = LoggerFactory.getLogger(JsonUtil::class.java)
  private val gson = Gson()

  val toTripEvent = MapFunction<String, TripEvent?> {
    val jsonMap: Map<String, Any?> = gson.fromJson<Map<String, Any?>>(it, Map::class.java)
    val body = jsonMap["body"] as Map<String, Any?>
    val message = body["message"] as Map<String, Any?>
    val messageType = message["type"] as String
    val id = (message["tripNumber"] as Double).toLong()
    val header = body["header"] as Map<String, Any?>
    val timestamp = Instant.from(ZonedDateTime.parse(header["timestamp"] as String))
    val longitude = header["latitude"] as Double
    val latitude = header["longitude"] as Double
    val gpsQuality: GpsQuality = GpsQuality.valueOf(header["fixQuality"] as String)
    when (messageType) {
      "TripStartEvent" -> TripStart(
          id,
          timestamp,
          latitude,
          longitude,
          gpsQuality,
          vehicleId = message["vin"] as String,
          internationalMobileEquipmentId = message["imei"] as String
      )
      "TripEndEvent" -> TripEnd(
          id,
          timestamp,
          latitude,
          longitude,
          gpsQuality,
          distanceTravelledInKilometers = message["distanceTravelled"] as Double,
          timeMovingInSeconds = message["tripTime"] as Double,
          timeIdleInSeconds = message["tripIdleTime"] as Double
      )
      "GPSMessage" -> GpsData(
          id,
          timestamp,
          latitude,
          longitude,
          gpsQuality
      )
      else -> {
        logger.error("Unknown trip event type: $messageType")
        null
      }
    }
  }
}
