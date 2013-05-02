module EM::Voldemort
  # Like HTTP 400 series responses, this exception is used for errors that are the client's fault.
  # The request generally should not be retried.
  class ClientError < RuntimeError; end

  # Exception to indicate that the requested key does not exist in the Voldemort store.
  class KeyNotFound < ClientError; end

  # Like HTTP 500 series responses, this exception is used for errors on the server side or in the
  # network. They are generally transient, so it makes sense to retry failed requests a limited
  # number of times.
  class ServerError < RuntimeError; end
end
