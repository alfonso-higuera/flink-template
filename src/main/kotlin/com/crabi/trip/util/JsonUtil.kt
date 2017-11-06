package com.crabi.trip.util

import com.crabi.trip.models.GpsData
import com.crabi.trip.models.GpsQuality
import com.crabi.trip.models.Trip
import com.crabi.trip.models.TripEnd
import com.crabi.trip.models.TripEvent
import com.crabi.trip.models.TripStart
import com.google.gson.Gson
import org.apache.flink.api.common.functions.MapFunction
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Instant
import java.time.ZonedDateTime

object JsonUtil {

  private val logger: Logger = LoggerFactory.getLogger(JsonUtil::class.java)
  private val gson = Gson()

  val tripToJson = MapFunction<Trip, String?> {
    try {
      gson.toJson(it)
    } catch (ex: Exception) {
      null
    }
  }

  val toTripEvent = MapFunction<String, TripEvent?> {
    try {
      val jsonMap: Map<String, Any?> = gson.fromJson<Map<String, Any?>>(it, Map::class.java)
      val header = jsonMap["header"] as Map<String, Any?>
      val deviceId = header["deviceId"] as String
      val body = jsonMap["body"] as Map<String, Any?>
      val message = body["message"] as Map<String, Any?>
      val messageType = message["type"] as String
      val bodyHeader = body["header"] as Map<String, Any?>
      val id = (bodyHeader["tripNumber"] as Double).toLong()
      val timestamp = Instant.from(ZonedDateTime.parse(bodyHeader["timestamp"] as String))
      val latitude = bodyHeader["latitude"] as Double
      val longitude = bodyHeader["longitude"] as Double
      val gpsQuality: GpsQuality = GpsQuality.valueOf(bodyHeader["fixQuality"] as String)
      when (messageType) {
        "TripStartEvent" -> TripStart(
            id,
            deviceId,
            timestamp,
            latitude,
            longitude,
            gpsQuality,
            vehicleId = message["vin"] as String,
            internationalMobileEquipmentId = message["imei"] as String
        )
        "TripEndEvent" -> TripEnd(
            id,
            deviceId,
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
            deviceId,
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
    } catch (ex: Exception) {
      null
    }
  }
}
