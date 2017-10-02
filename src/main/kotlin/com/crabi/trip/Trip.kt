package com.crabi.trip

import java.time.Instant
import java.io.Serializable

data class GpsPoint(
    val timestamp: Instant,
    val latitude: Double,
    val longitude: Double
) : Serializable

data class Trip(
    var id: Long?,
    var initialTimestamp: Instant?,
    var finalTimestamp: Instant?,
    var distanceTravelledInKilometers: Double?,
    var timeMovingInSeconds: Double?,
    var timeIdleInSeconds: Double?,
    var vehicleId: String?,
    var internationalMobileEquipmentId: String?,
    val gpsPoints: MutableCollection<GpsPoint>
) : Serializable

enum class GpsQuality { FixOk, StoredFix, FixInvalid, Unknown }

abstract class TripEvent(
    open val id: Long,
    open val timestamp: Instant,
    open val latitude: Double,
    open val longitude: Double,
    open val gpsQuality: GpsQuality
)

data class TripStart(
    override val id: Long,
    override val timestamp: Instant,
    override val latitude: Double,
    override val longitude: Double,
    override val gpsQuality: GpsQuality,
    val vehicleId: String,
    val internationalMobileEquipmentId: String
) : TripEvent(id, timestamp, latitude, longitude, gpsQuality)

data class TripEnd(
    override val id: Long,
    override val timestamp: Instant,
    override val latitude: Double,
    override val longitude: Double,
    override val gpsQuality: GpsQuality,
    val distanceTravelledInKilometers: Double,
    val timeMovingInSeconds: Double,
    val timeIdleInSeconds: Double
) : TripEvent(id, timestamp, latitude, longitude, gpsQuality)

data class GpsData(
    override val id: Long,
    override val timestamp: Instant,
    override val latitude: Double,
    override val longitude: Double,
    override val gpsQuality: GpsQuality
) : TripEvent(id, timestamp, latitude, longitude, gpsQuality)
