package com.crabi.trip

import java.time.Instant

abstract class Trip(
    open val id: Long,
    open val timestamp: Instant
)

data class TripStart(
    override val id: Long,
    override val timestamp: Instant,
    val odometerValue: Long,
    val vehicleProtocol: VehicleProtocol,
    val vehicleId: String
) : Trip(id, timestamp)

data class TripData(
    override val id: Long,
    override val timestamp: Instant,
    val parameterIdsData: Set<ParameterIdData>
) : Trip(id, timestamp)

data class TripEvent(
    override val id: Long,
    override val timestamp: Instant,
    val eventData: EventData
) : Trip(id, timestamp)

data class TripEnd(
    override val id: Long,
    override val timestamp: Instant,
    val odometerValue: Long,
    val fuelConsumed: Double
) : Trip(id, timestamp)
