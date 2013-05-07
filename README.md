EM::Voldemort
=============

A Ruby client for [Voldemort](http://www.project-voldemort.com/), implemented using
[EventMachine](http://rubyeventmachine.com/).

Features:

* High-performance, non-blocking access to Voldemort clusters.
* Fault-tolerant: automatically retries failed requests, routes requests to replica nodes if the
  primary node is down, reconnects when a connection is lost, etc.
* Supports client-side routing using the same consistent hashing algorithm as the Java client.
* Keys and values in Voldemort's [Binary JSON](https://github.com/voldemort/voldemort/wiki/Binary-JSON-Serialization)
  are automatically serialized and unserialized to Ruby hashes and arrays.
* Transparent gzip compression (like the Java client).

Limitations:

* Can only be used to access
  [read-only stores](https://github.com/voldemort/voldemort/wiki/Build-and-Push-Jobs-for-Voldemort-Read-Only-Stores)
  (the type of store that you build in a batch job in Hadoop and bulk-load into the Voldemort
  cluster). Accessing read-write stores (which use BDB or MySQL as storage engine) is not currently
  supported. This cuts out a lot of complexity (quorum reads/writes, conflict resolution, zoning
  etc).
* Currently only supports gzip or uncompressed data, none of the other compression codecs.
* Currently doesn't support serialization formats other than Binary JSON and raw bytes.

Compatibility:

* Ruby 1.9 and above (not compatible with 1.8).
* Only tested on MRI, but ought to work on any Ruby implementation.
* Should work with a wide range of Voldemort versions (this client uses the `pb0` Protocol
  Buffers-based protocol).


Usage
-----

`gem install em-voldemort` or add `gem 'em-voldemort'` to your Gemfile.

The client is initialized by giving it the hostname and port of any node in the cluster. That node
will be contacted to discover the other nodes and the configuration of the stores.
`EM::Voldemort::Cluster` is a client for an entire Voldemort cluster, which may have many stores;
`EM::Voldemort::Store` is the preferred way of accessing one particular store.

You get the store object from the cluster (you can do this during the initialization of your app --
EventMachine doesn't need to be running yet):

    MY_VOLDEMORT_CLUSTER = EM::Voldemort::Cluster.new(:host => 'voldemort.example.com', :port => 6666)
    MY_VOLDEMORT_STORE = MY_VOLDEMORT_CLUSTER.store('my_store')

    # Alternative convenience method, using a URL:
    MY_VOLDEMORT_STORE = EM::Voldemort::Store.from_url('voldemort://voldemort.example.com:6666/my_store')

Making requests is then straightforward:

    request = MY_VOLDEMORT_STORE.get('key-to-look-up')
    request.callback {|response| puts "value: #{response}" }
    request.errback {|error| puts "request failed: #{error}" }

On successful requests, the value passed to the callback is fully decoded (gzip decompressed and/or
Binary JSON decoded, if appropriate). On failed requests, an exception object is passed to the
errback. The exception object is of one of the following types:

* `EM::Voldemort::ClientError` -- like a HTTP 400 series error. Something is wrong with the request,
  and retrying it won't help.
* `EM::Voldemort::KeyNotFound` -- subclass of `ClientError`, indicates that the given key was not
  found in the store (like HTTP 404).
* `EM::Voldemort::ServerError` -- like a HTTP 500 series error or network error. We were not able to
  get a valid response from the cluster. This gem automatically retries requests, so there's no
  point in immediately retrying the request in application code (though you may want to retry after
  a delay, if your application allows).

If you want to gracefully shut down the client (which allows any requests in flight to complete, but
stops any further requests from being made):

    MY_VOLDEMORT_CLUSTER.close
