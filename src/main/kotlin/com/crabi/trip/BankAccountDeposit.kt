package com.crabi.trip

data class BankAccountDeposit
  (val id: Int, val amount: Double, val timestamp: Long, val isTerminator: Boolean) {

  companion object {
    fun of(input: String): BankAccountDeposit {
      val tokens: List<String> = input.split(delimiters = ":")
      return BankAccountDeposit(
          id = tokens[0].toInt(),
          amount = tokens[1].toDouble(),
          timestamp = tokens[2].toLong(),
          isTerminator = tokens[3].toBoolean()
      )
    }
  }
}
