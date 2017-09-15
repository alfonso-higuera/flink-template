package com.crabi.trip

import com.google.gson.annotations.SerializedName

data class BankAccountDeposit(
    val id: Int,
    val amount: Double,
    val timestamp: Long,
    @SerializedName("is_terminator") val isTerminator: Boolean
)
