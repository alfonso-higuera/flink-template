package com.crabi.trip

enum class VehicleProtocol {
  VPW1,
  PWM,
  ISO9141,
  ISO14230,
  ISO14230FastInit,
  CAN11Bit,
  CAN29Bit;

  companion object {
    fun of(name: String): VehicleProtocol {
      return VehicleProtocol.valueOf(name)
    }
  }
}