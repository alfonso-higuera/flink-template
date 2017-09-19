package com.crabi.trip

import java.time.Instant
import java.util.Arrays

abstract class ParameterIdData

data class MalfunctionIndicatorLamp(
    val wasCommandedToBeOn: Boolean,
    val totalAmountOfDiagnosticTroubleCodes: Long
) : ParameterIdData()

data class FreezeDiagnosticTroubleCode(val freezeFrameTroubleCode: String) : ParameterIdData()

data class FuelSystemStatus(
    val isClosedLoopOxygenSensorFaulty: Boolean,
    val isOpenLoopSystemWideFaulty: Boolean,
    val isOpenLoopDueDrivingConditions: Boolean,
    val isClosedLoopOxygenSensorUsedToControlFuel: Boolean,
    val isOpenLoopYetToBeSatisfied: Boolean
) : ParameterIdData()

data class CalculatedEngineLoad(val percentageOfPeekAvailableTorque: Int) : ParameterIdData()

data class EngineCoolantTemperature(val temperatureInCelsius: Int)
  : ParameterIdData()

data class FuelPercentTrim(
    val shortTermFuelPercentTrimInBankOne: Int,
    val shortTermFuelPercentTrimInBankTwo: Int,
    val longTermFuelPercentTrimInBankOne: Int,
    val longTermFuelPercentTrimInBankTwo: Int
) : ParameterIdData()

data class FuelPressure(val pressureInKiloPascals: Int) : ParameterIdData()

data class IntakeManifoldAbsolutePressure(val pressureInKiloPascals: Int) : ParameterIdData()

data class EngineRevolutionsPerMinute(val revolutionsPerMinute: Int): ParameterIdData()

data class VehicleSpeed(val speedInKilometerPerHour: Int): ParameterIdData()

data class fuelInjectionTiming(val angleRelativeToTopDeadCenter: Int) : ParameterIdData()

data class IntakeAirTemperature(val temperatureInCelsius: Int) : ParameterIdData()

data class MassAirFlowRate(val rateInGramsPerSecond: Double) : ParameterIdData()

data class ThrottlePosition(val positionInPercentage: Int) : ParameterIdData()

data class CommandedSecondaryAirStatus(
    val isPumpCommandedOnForDiagnostics: Boolean,
    val isFromOutsideAtmosphereOrOff: Boolean,
    val isDownstreamOfFirstCatalyticConverter: Boolean,
    val isUpstreamOfFirstCatalyticConverter: Boolean
) : ParameterIdData()

data class OxygenSensorsPresent(
    val isSensorOnePresent: Boolean,
    val isSensorTwoPresent: Boolean,
    val isSensorThreePresent: Boolean,
    val isSensorFourPresent: Boolean
) : ParameterIdData()

data class OxygenSensorData(val voltage: Double, val shortTermFuelTrim: Double)

data class BankOxygenSensorsData(val sensors: Array<OxygenSensorData>) {

  override fun equals(other: Any?): Boolean {
    if (this === other) return true
    if (javaClass != other?.javaClass) return false

    other as BankOxygenSensorsData

    if (!Arrays.equals(sensors, other.sensors)) return false

    return true
  }

  override fun hashCode(): Int {
    return Arrays.hashCode(sensors)
  }
}

data class OxygenSensorsData(
    val bankOxygenSensorsData: Array<BankOxygenSensorsData>
) : ParameterIdData() {

  override fun equals(other: Any?): Boolean {
    if (this === other) return true
    if (javaClass != other?.javaClass) return false

    other as OxygenSensorsData

    if (!Arrays.equals(bankOxygenSensorsData, other.bankOxygenSensorsData)) return false

    return true
  }

  override fun hashCode(): Int {
    return Arrays.hashCode(bankOxygenSensorsData)
  }
}

enum class OnBoardDiagnosticsStandard {
  NONE,
  OBDI,
  OBDII,
  OBD,
  OBDAndOBDII,
  EOBD
}

data class OnBoardDiagnosticsStandardConformation(
    val standards: Set<OnBoardDiagnosticsStandard>
) : ParameterIdData()

data class BankSensorsPresence(val sensors: BooleanArray) {

  override fun equals(other: Any?): Boolean {
    if (this === other) return true
    if (javaClass != other?.javaClass) return false

    other as BankSensorsPresence

    if (!Arrays.equals(sensors, other.sensors)) return false

    return true
  }

  override fun hashCode(): Int {
    return Arrays.hashCode(sensors)
  }
}

data class OxygenSensorsPresenceByBank(
    val bankSensorPresence: Array<BankSensorsPresence>
) : ParameterIdData() {

  override fun equals(other: Any?): Boolean {
    if (this === other) return true
    if (javaClass != other?.javaClass) return false

    other as OxygenSensorsPresenceByBank

    if (!Arrays.equals(bankSensorPresence, other.bankSensorPresence)) return false

    return true
  }

  override fun hashCode(): Int {
    return Arrays.hashCode(bankSensorPresence)
  }
}

data class RuntimeSinceEngineStarted(val timeInSeconds: Int) : ParameterIdData()

data class DistanceTraveledWithMalfunctionIndicatorLampOn(val distanceInKilometers: Int)
  : ParameterIdData()

data class FuelRailPressureRelativeToManifoldVacuum(val pressureInKiloPascals: Int)
  : ParameterIdData()

data class FuelRailPressure(val pressureInKiloPascals: Int) : ParameterIdData()

data class EquivalenceRatioVoltage(val ratio: Double, val voltage: Int)

data class OxygenEquivalenceRatioVoltage(val sensors: Array<EquivalenceRatioVoltage>)
  : ParameterIdData() {

  override fun equals(other: Any?): Boolean {
    if (this === other) return true
    if (javaClass != other?.javaClass) return false

    other as OxygenEquivalenceRatioVoltage

    if (!Arrays.equals(sensors, other.sensors)) return false

    return true
  }

  override fun hashCode(): Int {
    return Arrays.hashCode(sensors)
  }
}

data class CommandedExhaustGasRecirculation(val commandedOpennessPercentage: Int)
  : ParameterIdData()

data class ExhaustGasRecirculationError(val positionErrorPercentage: Int) : ParameterIdData()

data class CommandedEvaporativePurge(val commandedOpennessPercentage: Int) : ParameterIdData()

data class FuelLevelInput(val tankLevelPercentage: Int) : ParameterIdData()

data class WarmUpsSinceCodesWereCleared(val amount: Int) : ParameterIdData()

data class DistanceTraveledSinceCodesWereCleared(val distanceInKilometers: Int) : ParameterIdData()

data class EvaporativeSystemVaporPressure(val pressureInPascals: Int) : ParameterIdData()

data class BarometricPressure(val pressureInKiloPascals: Int) : ParameterIdData()

data class EquivalentRatioCurrent(val ratio: Double, val current: Double)

data class OxygenSensorsEquivalentRatioCurrent(val sensors: Array<EquivalentRatioCurrent>)
  : ParameterIdData() {

  override fun equals(other: Any?): Boolean {
    if (this === other) return true
    if (javaClass != other?.javaClass) return false

    other as OxygenSensorsEquivalentRatioCurrent

    if (!Arrays.equals(sensors, other.sensors)) return false

    return true
  }

  override fun hashCode(): Int {
    return Arrays.hashCode(sensors)
  }
}

data class CatalystTemperatureData(val temperatureInCelsius: Double)

data class CatalystTemperatureBySensor(val sensors: Array<CatalystTemperatureData>) {

  override fun equals(other: Any?): Boolean {
    if (this === other) return true
    if (javaClass != other?.javaClass) return false

    other as CatalystTemperatureBySensor

    if (!Arrays.equals(sensors, other.sensors)) return false

    return true
  }

  override fun hashCode(): Int {
    return Arrays.hashCode(sensors)
  }
}

data class CatalystTemperature(val banks: Array<CatalystTemperatureBySensor>)
  : ParameterIdData() {

  override fun equals(other: Any?): Boolean {
    if (this === other) return true
    if (javaClass != other?.javaClass) return false

    other as CatalystTemperature

    if (!Arrays.equals(banks, other.banks)) return false

    return true
  }

  override fun hashCode(): Int {
    return Arrays.hashCode(banks)
  }
}

data class MonitorStatusInThisDriveCycle(
    val isMisfireTestAvailable: Boolean,
    val isFuelSystemTestAvailable: Boolean,
    val isComponentsTestAvailable: Boolean,
    val isCatalystTestAvailable: Boolean,
    val isHeatedCatalystTestAvailable: Boolean,
    val isEvaporativeSystemTestAvailable: Boolean,
    val isSecondaryAirSystemTestAvailable: Boolean,
    val isAcRefrigerantTestAvailable: Boolean,
    val isOxygenSensorTestAvailable: Boolean,
    val isOxygenSensorHeaterTestAvailable: Boolean,
    val isExhaustGasRecirculationSystemTestAvailable: Boolean,
    val isMisfireTestComplete: Boolean,
    val isFuelSystemTestComplete: Boolean,
    val isComponentsTestComplete: Boolean,
    val isCatalystTestComplete: Boolean,
    val isHeatedCatalystTestComplete: Boolean,
    val isEvaporativeSystemTestComplete: Boolean,
    val isSecondaryAirSystemTestComplete: Boolean,
    val isAcRefrigerantTestComplete: Boolean,
    val isOxygenSensorTestComplete: Boolean,
    val isOxygenSensorHeaterTestComplete: Boolean,
    val isExhaustGasRecirculationSystemTestComplete: Boolean
) : ParameterIdData()

data class ControlModuleVoltage(val voltage: Int) : ParameterIdData()

data class AbsoluteLoadValue(val airMassOfIntakeStrokeInPercent: Int) : ParameterIdData()

data class CommandedEquivalenceRatio(val airFuelEquilibriumPercentage: Double) : ParameterIdData()

data class RelativeThrottlePosition(val opennessOfThrottle: Int) : ParameterIdData()

data class AmbientAirTemperature(val temperatureInCelsius: Int) : ParameterIdData()

data class AbsoluteThrottlePosition(val opennessOfThrottleB: Int, val opennessOfThrottleC: Int)
  : ParameterIdData()

data class AcceleratorPedalPosition(
    val positionPercentD: Int,
    val positionPercentE: Int,
    val positionPercentF: Int
) : ParameterIdData()

data class CommandedThrottleActuator(val opennessOfThrottlePercentage: Int) : ParameterIdData()

data class TimeTraveledWithMalfunctionIndicatorLampOn(val timeInMinutes: Int) : ParameterIdData()

data class TimeSinceTroubleCodesCleared(val timeInMinutes: Int) : ParameterIdData()

data class MaximumValues(
    val ofEquivalenceRatio: Double,
    val ofOxygenSensorVoltage: Int,
    val ofOxygenSensorCurrentInMilliAmperes: Double,
    val ofIntakeManifoldAbsolutePressureInKiloPascals: Int
) : ParameterIdData()

data class MaximumAirFlowRateFromManifoldSensor(val airFlowInGramsPerSecond: Int)
  : ParameterIdData()

enum class FuelType {
  Gasoline,
  Methanol,
  Ethanol,
  Diesel,
  LiquefiedPetroleumGas,
  CompressedNaturalGas,
  Propane,
  Electricity,
  BiFuelRunningGasoline,
  BiFuelRunningMethanol,
  BiFuelRunningEthanol,
  BiFuelRunningLiquefiedPetroleumGas,
  BiFuelRunningCompressedNaturalGas,
  BiFuelRunningPropane,
  BiFuelRunningElectricity,
  HybridGasolineElectricity,
  HybridGasoline,
  HybridEthanol,
  HybridDiesel,
  HybridElectricity,
  HybridMixed,
  HybridRegenerative
}

data class AlcoholFuelDetected(val percentage: Int) : ParameterIdData()

data class AbsoluteEvaporativeSystemVaporPressure(val pressureInKiloPascals: Double)
  : ParameterIdData()

data class AbsoluteFuelRailPressure(val pressureInKiloPascals: Int) : ParameterIdData()

data class RelativeAcceleratorGasPedalPosition(val pedalPositionPercentage: Int): ParameterIdData()

data class HybridBatteryPackRemainingLife(val remainingLifePercentage: Int) : ParameterIdData()

data class EngineOilTemperature(val temperatureInCelsius: Int) : ParameterIdData()

data class FuelInjectionTiming(val angleRelativeToTopDeadCenter: Double) : ParameterIdData()

data class EngineFuelRate(val consumptionInLitersPerHour: Double) : ParameterIdData()

data class Point(val x: Int, val y: Int, val z: Int)

data class RawAccelerometerData(val offsetPoints: Collection<Point>, val basePoint: Point)

data class RawAccelerometer(val data: Collection<RawAccelerometerData>) : ParameterIdData()

data class NormalizedAccelerometer(val offsets: List<Point>, val base: Point) : ParameterIdData()

data class FuelLevelInputSmoothed(val tankLevelPercentage: Int) : ParameterIdData()

data class GpsPoint(val latitude: Double, val longitude: Double)

data class GpsData(
    val headingAngle: Int,
    val horizontalDilutionOfPrecision: Int,
    val gpsPoint: GpsPoint,
    val numberOfSatellites: Int,
    val gpsRegion: GpsRegion,
    val fixQuality: GpsFixQuality
) : ParameterIdData()

data class GpsDataAggregate(val timestamp: Instant, val gpsData: GpsData) {

  companion object {
    val timestampComparator = Comparator<GpsDataAggregate> {
      first, second ->
      first.timestamp.compareTo(second.timestamp)
    }
  }
}
