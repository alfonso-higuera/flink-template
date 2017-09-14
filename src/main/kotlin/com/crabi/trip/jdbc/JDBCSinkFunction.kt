package com.crabi.trip.jdbc

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.FunctionInitializationContext
import org.apache.flink.runtime.state.FunctionSnapshotContext
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.types.Row

class JDBCSinkFunction(private val outputFormat: JDBCOutputFormat)
  : RichSinkFunction<Row>(), CheckpointedFunction {

  override fun invoke(row: Row): Unit {
    outputFormat.writeRecord(row)
  }

  override fun initializeState(context: FunctionInitializationContext): Unit { }

  override fun snapshotState(context: FunctionSnapshotContext?): Unit {
    outputFormat.flush()
  }

  override fun open(parameters: Configuration?): Unit {
    super.open(parameters)
    val context: RuntimeContext = runtimeContext
    outputFormat.runtimeContext = context
    outputFormat.open(context.indexOfThisSubtask, context.numberOfParallelSubtasks)
  }

  override fun close(): Unit {
    outputFormat.close()
    super.close()
  }
}
