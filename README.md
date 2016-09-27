#SparkInternal
/**
 * This package implements Spark's memory management system. This system consists of two main
 * components, a JVM-wide memory manager and a per-task manager:
 *
 *  - [[org.apache.spark.memory.MemoryManager]] manages Spark's overall memory usage within a JVM.
 *    This component implements the policies for dividing the available memory across tasks and for
 *    allocating memory between storage (memory used caching and data transfer) and execution
 *    (memory used by computations, such as shuffles, joins, sorts, and aggregations).
 *  - [[org.apache.spark.memory.TaskMemoryManager]] manages the memory allocated by individual
 *    tasks. Tasks interact with TaskMemoryManager and never directly interact with the JVM-wide
 *    MemoryManager.
 *
 * Internally, each of these components have additional abstractions for memory bookkeeping:
 *
 *  - [[org.apache.spark.memory.MemoryConsumer]]s are clients of the TaskMemoryManager and
 *    correspond to individual operators and data structures within a task. The TaskMemoryManager
 *    receives memory allocation requests from MemoryConsumers and issues callbacks to consumers
 *    in order to trigger spilling when running low on memory.
 *  - [[org.apache.spark.memory.MemoryPool]]s are a bookkeeping abstraction used by the
 *    MemoryManager to track the division of memory between storage and execution.
 *
 * Diagrammatically:
 *
 * {{{
 *       +-------------+
 *       | MemConsumer |----+                                   +------------------------+
 *       +-------------+    |    +-------------------+          |     MemoryManager      |
 *                          +--->| TaskMemoryManager |----+     |                        |
 *       +-------------+    |    +-------------------+    |     |  +------------------+  |
 *       | MemConsumer |----+                             |     |  |  StorageMemPool  |  |
 *       +-------------+         +-------------------+    |     |  +------------------+  |
 *                               | TaskMemoryManager |----+     |                        |
 *                               +-------------------+    |     |  +------------------+  |
 *                                                        +---->|  |OnHeapExecMemPool |  |
 *                                        *               |     |  +------------------+  |
 *                                        *               |     |                        |
 *       +-------------+                  *               |     |  +------------------+  |
 *       | MemConsumer |----+                             |     |  |OffHeapExecMemPool|  |
 *       +-------------+    |    +-------------------+    |     |  +------------------+  |
 *                          +--->| TaskMemoryManager |----+     |                        |
 *                               +-------------------+          +------------------------+
 * }}}
 *
 *
 * There are two implementations of [[org.apache.spark.memory.MemoryManager]] which vary in how
 * they handle the sizing of their memory pools:
 *
 *  - [[org.apache.spark.memory.UnifiedMemoryManager]], the default in Spark 1.6+, enforces soft
 *    boundaries between storage and execution memory, allowing requests for memory in one region
 *    to be fulfilled by borrowing memory from the other.
 *  - [[org.apache.spark.memory.StaticMemoryManager]] enforces hard boundaries between storage
 *    and execution memory by statically partitioning Spark's memory and preventing storage and
 *    execution from borrowing memory from each other. This mode is retained only for legacy
 *    compatibility purposes.
 */