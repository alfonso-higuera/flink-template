package com.crabi.trip

import java.time.Instant
import java.util.SortedSet

data class GpsPointAggregate(val timestamp: Instant, val gpsPoint: GpsPoint) {

  companion object {
    val timestampComparator = Comparator<GpsPointAggregate> {
      first, second ->
        first.timestamp.compareTo(second.timestamp)
    }
  }
}

data class TripAggregation(
    var id: Long,
    var timestamps: SortedSet<Instant>,
    var gpsPoints: SortedSet<GpsPointAggregate>,
    var speeds: SortedSet<VehicleSpeedAggregate>,
    var totalTimeDurationInSeconds: Double,
    var timeDurationMovingInSeconds: Double,
    var timeDurationStoppedInSeconds: Double,
    var vehicleId: String,
    var distanceInKilometers: Double
)

data class VehicleSpeedAggregate(val timestamp: Instant, val vehicleSpeed: VehicleSpeed) {

  companion object {
    val timestampComparator = Comparator<VehicleSpeedAggregate> {
      first, second ->
        first.timestamp.compareTo(second.timestamp)
    }
  }
}