package com.crabi.trip

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.apache.flink.api.common.functions.MapFunction
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.lang.reflect.Type
import java.time.OffsetDateTime

object JSONUtil {

  private val logger: Logger = LoggerFactory.getLogger(JSONUtil::class.java)
  private val gson = Gson()

  private fun toParameterIdsData(data: Map<String, Any?>): Set<ParameterIdData> {
    val parameterIdsOrNull: List<ParameterIdData?> = data.map { (name, value) ->
      when (name) {
        "EngineCoolantTemp" ->
          EngineCoolantTemperature(temperatureInCelsius = (value as Double).toInt())
        "EngineRpm" ->
          EngineRevolutionsPerMinute(revolutionsPerMinute = (value as Double).toInt())
        "MilStatus" -> {
          val m: Map<String, Any?> = value as Map<String, Any?>
          MalfunctionIndicatorLamp(
              wasCommandedToBeOn = m["commandedOn"] as Boolean,
              totalAmountOfDiagnosticTroubleCodes = (m["numCodes"] as Double).toLong()
          )
        }
        "FreezeFrameTroubleCode" ->
          FreezeDiagnosticTroubleCode(freezeFrameTroubleCode = value as String)
        "FuelStatus" -> {
          val m: Map<String, Any?> = value as Map<String, Any?>
          FuelSystemStatus(
              isClosedLoopOxygenSensorFaulty = m["closedLoopO2SensorFault"] as Boolean,
              isClosedLoopOxygenSensorUsedToControlFuel =
                m["closedLoopO2SensorFuelControl"] as Boolean,
              isOpenLoopDueDrivingConditions = m["openLoopDrivingConditions"] as Boolean,
              isOpenLoopSystemWideFaulty = m["openLoopDrivingConditions"] as Boolean,
              isOpenLoopYetToBeSatisfied = m["openLoopNotYetSatisfied"] as Boolean
          )
        }
        "CalcEngineLoad" ->
          CalculatedEngineLoad(percentageOfPeekAvailableTorque = (value as Double).toInt())
        "FuelPressure" ->
          FuelPressure(pressureInKiloPascals = (value as Double).toInt())
        "IntakeManifoldAbsPressure" ->
          IntakeManifoldAbsolutePressure(pressureInKiloPascals = (value as Double).toInt())
        "VehicleSpeed" ->
          VehicleSpeed(speedInKilometerPerHour = (value as Double).toInt())
        "TimingAdvance" ->
          FuelInjectionTiming(angleRelativeToTopDeadCenter = value as Double)
        "IntakeAirTemp" ->
          IntakeAirTemperature(temperatureInCelsius = (value as Double).toInt())
        "MafAirFlowRate" ->
          MassAirFlowRate(rateInGramsPerSecond = value as Double)
        "ThrottlePosition" ->
          ThrottlePosition(positionInPercentage = (value as Double).toInt())
        "CommandedSecondaryAirStatus" -> {
          val m: Map<String, Any?> = value as Map<String, Any?>
          CommandedSecondaryAirStatus(
              isPumpCommandedOnForDiagnostics = m["pumpCommandedOnForDiagnostics"] as Boolean,
              isDownstreamOfFirstCatalyticConverter =
                m["downstreamOfFirstCatalyticConverter"] as Boolean,
              isFromOutsideAtmosphereOrOff = m["fromOutsideAtmosphereOrOff"] as Boolean,
              isUpstreamOfFirstCatalyticConverter =
                m["upstreamOfFirstCatalyticConverter"] as Boolean
          )
        }
        "RawAccelerometer" -> {
          val rawAccelerometerJsonMap: List<Map<String, Any?>> = value as List<Map<String, Any?>>
          val rawAccelerometerData: Collection<RawAccelerometerData> =
              rawAccelerometerJsonMap.map {
                val offsetsJsonMap: List<Map<String, Any?>> =
                    it["offsets"] as List<Map<String, Any?>>
                val offsetPoints: Collection<Point> = offsetsJsonMap.map {
                  Point(
                      x = (it["x"] as Double).toInt(),
                      y = (it["y"] as Double).toInt(),
                      z = (it["z"] as Double).toInt()
                  )
                }
                val basePointJsonMap: Map<String, Any?> = it["base"] as Map<String, Any?>
                val basePoint = Point(
                    x = (basePointJsonMap["x"] as Double).toInt(),
                    y = (basePointJsonMap["y"] as Double).toInt(),
                    z = (basePointJsonMap["z"] as Double).toInt()
                )
                RawAccelerometerData(offsetPoints, basePoint)
              }
          RawAccelerometer(data = rawAccelerometerData)
        }
        "GpsReading" -> {
          val m: Map<String, Any?> = value as Map<String, Any?>
          GpsData(
              headingAngle = (m["heading"] as Double).toInt(),
              horizontalDilutionOfPrecision = (m["horizontalDilutionOfPrecision"] as Double).toInt(),
              gpsPoint = GpsPoint(
                  latitude = m["latitude"] as Double,
                  longitude = m["longitude"] as Double
              ),
              numberOfSatellites = (m["numberOfSatellites"] as Double).toInt(),
              gpsRegion = GpsRegion.of(m["hemisphere"] as String),
              fixQuality = GpsFixQuality.of(m["fixQuality"] as String)
          )
        }
        else -> {
          null
        }
      }
    }
    return parameterIdsOrNull
        .filter { it != null }
        .map { it as ParameterIdData }
        .toSet()
  }

  private fun toEventData(data: Map<String, Any?>): EventData? {
    val eventTypeStr: String = data["type"] as String
    if (eventTypeStr == "ObdSpeedEvent") {
      val onBoardDiagnosticsSpeedEventDataJsonMap: Map<String, Any?> =
          data["obdSpeedEventData"] as Map<String, Any?>
      if (onBoardDiagnosticsSpeedEventDataJsonMap["type"] as String == "SpeedMetricsPerTrip") {
        val tripSpeedMetrics = TripSpeedMetrics(
            distanceInKilometers =
              onBoardDiagnosticsSpeedEventDataJsonMap["tripDistance"] as Double,
            timeDurationInSeconds =
              onBoardDiagnosticsSpeedEventDataJsonMap["tripTimeSeconds"] as Double
        )
        return OnBoardDiagnosticsSpeedEvent(data = tripSpeedMetrics)
      }
    }
    return null
  }

  val toTrip = MapFunction<String, Trip?> {
    try {
      val type: Type = object : TypeToken<Map<String, Any?>>(){}.type
      val jsonMap: Map<String, Any?> = gson.fromJson<Map<String, Any?>>(it, type)
      val body: Map<String, Any?> = jsonMap["body"] as Map<String, Any?>
      val tripNumber: Long = (body["tripNumber"] as Double).toLong()
      val timestamp: String = body["timestamp"] as String
      val messageType: String = body["type"] as String
      val trip: Trip? =
          when (messageType) {
            "TripStartRelativeTime" ->
              TripStart(
                  id = tripNumber,
                  timestamp = OffsetDateTime.parse(timestamp).toInstant(),
                  odometerValue = (body["odometer"] as Double).toLong(),
                  vehicleProtocol = VehicleProtocol.of(name = body["vehicleProtocol"] as String),
                  vehicleId = body["vin"] as String
              )
            "TripData" ->
              TripData(
                  id = tripNumber,
                  timestamp = OffsetDateTime.parse(timestamp).toInstant(),
                  parameterIdsData =
                    toParameterIdsData(data = body["pidData"] as Map<String, Any?>)
              )
            "TripEnd" ->
              TripEnd(
                  id = tripNumber,
                  timestamp = OffsetDateTime.parse(timestamp).toInstant(),
                  odometerValue = (body["odometer"] as Double).toLong(),
                  fuelConsumed = body["fuelConsumed"] as Double
              )
            "TripEventRelativeTime" -> {
              val eventData: EventData? =
                  toEventData(data = body["eventData"] as Map<String, Any?>)
              if (eventData != null) {
                TripEvent(
                    id = tripNumber,
                    timestamp = OffsetDateTime.parse(timestamp).toInstant(),
                    eventData = eventData
                )
              } else {
                null
              }
            }
            else -> {
              throw IllegalArgumentException("Unknown event type: $messageType")
            }
          }
      trip
    } catch (ex: Exception) {
      logger.warn("Received malformed JSON: $it: $ex")
      null
    }
  }
}
