module EM::Voldemort
  # Provides access to one particular store on a Voldemort cluster. Deals with encoding of keys and
  # values.
  class Store

    attr_reader :store_name

    # Internal -- don't call this from application code. Use {Cluster#store} instead.
    def initialize(cluster, store_name)
      @cluster = cluster
      @store_name = store_name.to_s
    end

    def load_config(xml)
      # TODO
    end

  end
end
