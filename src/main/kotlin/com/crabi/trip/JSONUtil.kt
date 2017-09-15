package com.crabi.trip

import com.google.gson.Gson
import org.apache.flink.api.common.functions.MapFunction

object JSONUtil {

  private val gson = Gson()

  val toBankAccountDeposit = MapFunction<String, BankAccountDeposit> {
    gson.fromJson(it, BankAccountDeposit::class.java)
  }
}
