package com.crabi.trip

data class MyData
  (val key: String, val value: Long, val timestamp: Long, val isTerminator: Boolean) {

  companion object {
    fun of(input: String): MyData {
      val tokens: List<String> = input.split(delimiters = ":")
      return MyData(
          key = tokens[0],
          value = tokens[1].toLong(),
          timestamp = tokens[2].toLong(),
          isTerminator = tokens[3].toBoolean()
      )
    }
  }
}
