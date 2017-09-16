package com.crabi.trip

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.streaming.api.windowing.triggers.Trigger
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow

class ProcessingTimeTrigger<T : Any, W : GlobalWindow>
  (val minimumRetentionTimeInMilliseconds: Long, val maximumRetentionTimeInMilliseconds: Long) : Trigger<T, W>() {

  private val cleanupTimeStateDescription =
      ValueStateDescriptor<Long>("haveSeenElement", BasicTypeInfo.LONG_TYPE_INFO)

  override fun onProcessingTime(
      timestamp: Long, window: W, triggerContext: TriggerContext): TriggerResult {
    val cleanupTime: Long? =
        triggerContext.getPartitionedState(cleanupTimeStateDescription).value()
    if (cleanupTime != null && timestamp == cleanupTime) {
      clear(window, triggerContext)
      return TriggerResult.FIRE_AND_PURGE
    }
    return TriggerResult.CONTINUE
  }

  override fun clear(window: W, triggerContext: TriggerContext): Unit {
    triggerContext.getPartitionedState(cleanupTimeStateDescription).clear()
  }

  override fun onElement(
      element: T, timestamp: Long, window: W, triggerContext: TriggerContext): TriggerResult {
    val currentTime: Long = triggerContext.currentProcessingTime
    val cleanupTime: Long? = triggerContext.getPartitionedState(cleanupTimeStateDescription).value()
    if (cleanupTime == null || (currentTime + minimumRetentionTimeInMilliseconds) > cleanupTime) {
      val newCleanupTime: Long = currentTime + maximumRetentionTimeInMilliseconds
      triggerContext.registerProcessingTimeTimer(newCleanupTime)
      triggerContext
          .getPartitionedState(cleanupTimeStateDescription)
          .update(newCleanupTime)
    }
    return TriggerResult.CONTINUE
  }

  override fun onEventTime(
      timestamp: Long, window: W, triggerContext: TriggerContext): TriggerResult {
    return TriggerResult.CONTINUE
  }

  override fun canMerge(): Boolean {
    return false
  }

  override fun toString(): String {
    return "ProcessingTimeTrigger()"
  }

  override fun equals(other: Any?): Boolean {
    if (this === other) {
      return true
    }

    if (javaClass != other?.javaClass) {
      return false
    }

    other as ProcessingTimeTrigger<*, *>

    return this.cleanupTimeStateDescription == other.cleanupTimeStateDescription &&
        this.maximumRetentionTimeInMilliseconds == other.maximumRetentionTimeInMilliseconds &&
        this.minimumRetentionTimeInMilliseconds == other.minimumRetentionTimeInMilliseconds
  }

  override fun hashCode(): Int {
    return cleanupTimeStateDescription.hashCode()
  }
}
