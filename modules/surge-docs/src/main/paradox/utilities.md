# Utilities
The following utilities may be useful when developing Surge applications.

## MDC Logging
- The Mapped Diagnostic Context(MDC) allows us to put context to all log statements for a single thread.
- It is exposed via **org.slf4j.MDC** if you are using slf4j but other logging systems (such as log4j and logback) have similar concepts that slf4j wraps.

## MDC propagation across Futures
- To propagate MDC across Futures, first we need to import the **mdcExecutionContext** which copies the MDC to any new thread that is being used giving us multi-thread MDC logging.
```scala
import surge.internal.utils.MdcExecutionContext.mdcExecutionContext
```
By default, **mdcExecutionContext** wraps the default **global** ExecutionContext with MDC support in it. But if you want your mdcExecutionContext to use your own **ExecutionContext**, then you can do so by passing its object in the **MdcFuturePropagation** constructor:
```scala
  implicit lazy val mdcExecutionContext: MdcFuturePropagation = new MdcFuturePropagation(yourOwnExecutionContext)
```
**Note:** MdcFuturePropagation is exposed via **surge.internal.utils.MdcFuturePropagation**
- Next, we need to put the context in our log statements:
```scala
MDC.put("requestId", UUID.randomUUID().toString())
```
That's all. Using these two simple steps, we can propagate our MDC from the caller thread to the execution thread.
