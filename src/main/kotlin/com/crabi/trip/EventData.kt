package com.crabi.trip

abstract class EventData

enum class AccelerometerDataType { Triggered, Histogram, Unknown }

enum class TriggeredAxis {
  PositiveXAxis,
  NegativeXAxis,
  PositiveYAxis,
  NegativeYAxis,
  PositiveZAxis,
  NegativeZAxis
}

data class AccelerometerData(
    val type: AccelerometerDataType,
    val triggeredAxis: TriggeredAxis,
    val samples: List<Point>
)

data class AccelerometerEvent(
    val secondsRelativeToTriggerInSeconds: Int,
    val data: AccelerometerData
) : EventData()

abstract class FenceEventData

enum class TimeFenceEventDataType { Start, End }

data class TimeFenceEventData(
    val type: TimeFenceEventDataType,
    val tripId: Int,
    val distanceTraveled: Double,
    val durationInMinutes: Int
) : FenceEventData()

enum class GeoFenceEventDataType { Entry, Exit }

data class GeoFenceEventData(
    val type: GeoFenceEventDataType,
    val geoFenceId: Int
) : FenceEventData()

data class FenceEvent(val data: FenceEventData) : EventData()

enum class GpsRegion {
  NorthWest, NorthEast, SouthWest, SouthEast;

  companion object {
    fun of(name: String): GpsRegion {
      return GpsRegion.valueOf(name)
    }
  }
}

enum class GpsFixQuality {
  NoFix, Standard, Differential;

  companion object {
    fun of(name: String): GpsFixQuality {
      return GpsFixQuality.valueOf(name)
    }
  }
}

abstract class GpsEventData(
    open val heading: Int,
    open val horizontalDilutionOfPrecision: Int,
    open val latitude: Double,
    open val longitude: Double,
    open val numberOfSatellites: Int,
    open val gpsRegion: GpsRegion,
    open val gpsFixQuality: GpsFixQuality
)

data class TripGpsEvent(val data: GpsEventData) : EventData()
