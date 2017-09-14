package com.crabi.trip

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.ReducingState
import org.apache.flink.api.common.state.ReducingStateDescriptor
import org.apache.flink.api.common.typeutils.base.BooleanSerializer
import org.apache.flink.streaming.api.windowing.triggers.Trigger
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult
import org.apache.flink.streaming.api.windowing.windows.Window

class FirstElementWithPropertyTrigger<T : Any, W : Window>
  private constructor(private val predicateFn: (T) -> Boolean) : Trigger<T, W>() {

  companion object {

    fun <T : Any, W : Window> of(predicateFn: (T) -> Boolean)
        : FirstElementWithPropertyTrigger<T, W> {
      return FirstElementWithPropertyTrigger(predicateFn)
    }
  }

  private val Or = ReduceFunction<Boolean> { first: Boolean, second: Boolean -> first || second }
  private val stateDescription: ReducingStateDescriptor<Boolean> =
      ReducingStateDescriptor("hasSeenElement", Or, BooleanSerializer.INSTANCE)

  override fun onProcessingTime(
      timestamp: Long, window: W, triggerContext: TriggerContext): TriggerResult {
    return TriggerResult.CONTINUE
  }

  override fun clear(window: W, triggerContext: TriggerContext) {
    // This trigger do not have state. Hence, there is nothing to clear.
  }

  override fun onElement(
      element: T, timestamp: Long, window: W, triggerContext: TriggerContext): TriggerResult {
    val haveSeenElement: ReducingState<Boolean> =
        triggerContext.getPartitionedState(stateDescription)
    haveSeenElement.add(predicateFn(element))
    if (haveSeenElement.get()) {
      return TriggerResult.FIRE
    }
    return TriggerResult.CONTINUE
  }

  override fun onEventTime(
      timestamp: Long, window: W, triggerContext: TriggerContext): TriggerResult {
    return TriggerResult.CONTINUE
  }

  override fun canMerge(): Boolean {
    return true
  }

  override fun onMerge(window: W, onMergeContext: OnMergeContext) {
    onMergeContext.mergePartitionedState(stateDescription)
  }

  override fun toString(): String {
    return "FirstElementWithPropertyTrigger(item=$predicateFn)"
  }

  override fun equals(other: Any?): Boolean {
    if (this === other) return true
    if (javaClass != other?.javaClass) return false

    other as FirstElementWithPropertyTrigger<*, *>

    if (predicateFn != other.predicateFn) return false

    return true
  }

  override fun hashCode(): Int {
    return predicateFn.hashCode()
  }
}
