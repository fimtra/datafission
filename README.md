#What is it?
*Datafission is a framework for building real-time distributed data systems.* 

##How it works (the 20 second version)

![](https://github.com/fimtra/datafission/blob/master/docs/images/datafission%20summary.png) 

  * All data is represented as a *record*
  * Records hold data as key-value pairs
  * Records are assembled within a *context*
  * A context notifies *record listeners* when records change
  * Record listeners only receive the key-value changes in a record (only the deltas)
  * A *publisher* attaches to a context and publishes record changes to *proxy contexts*
  * A proxy context connects remotely to a single publisher to access records in the remote context
  * A context can publish RPCs that can be invoked remotely by a proxy context

##What are its key features?
  * Remote data subscription
  * Image-on-subscribe semantics
  * Atomic data changes (deltas)
  * RPC capability
  * Optimised threading model
  * Built-in TCP connectivity using NIO
  * Connection resilience
