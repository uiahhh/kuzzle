# Kuzzle Architecture

## Global overview

![archi_fonctionnal](images/kuzzle_functional_architecture.png)

Kuzzle Kernel API can be accessed from 3 different paths:

1. a [RESTFul API](API.REST.md)
2. a [Websocket connexion](API.WebSocket.md), using Kuzzle [Javascript SDK](http://kuzzleio.github.io/sdk-documentation/)
3. or any other custom protocol, using a Protocol Plugin (examples: [AMQP](API.AMQP.md), [MQTT](API.MQTT.md), [STOMP](API.STOMP.md))

In the background, Kuzzle uses:

* a noSQL engine to store, index and search contents (we use Elasticsearch by default).
* a cache engine to store subscription lists (we use redis by default).

## Core architecture

Focus on the above "Kuzzle kernel":
![archi_core](images/kuzzle_core_architecture.png)

### Main core components

* **Router Controller**: implements the 3 API routers, normalizes the input message and sends them to the Funnel Controller
* **Funnel Controller**: analyses the input message and calls the appropriate controller (see [API specification](api-specifications.md))
* **Admin Controller**, **Auth Controller**, **Bulk Controller**, **Write Controller**, **Subscribe Controller**, **Read Controller**: handles the input message (see [API documentation](http://kuzzleio.github.io/kuzzle-api-documentation))
* **Internal Components**: Any component used internally by controllers and any other internal component to interact with services


### Hooks

Hooks allow to attach actions to Kuzzle events.

For example, Admin, Bulk and Writer controllers emit a "data:create" event to handle some writing actions through the storage engine.
This event will trigger the execution of the *add* method and of the *write* hook, which will send the received message to the internal broker.

Then it is possible to implement custom hooks to trigger any event emitted by Kuzzle.

_For more details, see [hooks description](../lib/hooks/README.md)_

### Services

In Kuzzle, a Service module is the implementation of the interface to external services used by the application (internal broker, storage engine, cache engine, etc.)

_For more details, see [services description](../lib/services/README.md)_

### Workers

A worker is an independant component, detachable from a Kuzzle server container. It can be run in another container or even on another machine.

Workers attach themselves to the internal broker service fed by Kuzzle to perform any kind of task.

For instance, writing persistent data on Kuzzle is implemented as a write worker.

Additionally, serveral Workers of the same type can be launched in parallel, on the same or on a different host.

This flexibility allows administrators of Kuzzle system to leverage their resource consumption and distribute and/or scale their services to better fit their needs.


_For more details, see [workers description](../lib/workers/README.md)_

## Next steps

See [Request Scenarios documentation](request_scenarios/README.md) to see how these components are used together to handle a client's action.
