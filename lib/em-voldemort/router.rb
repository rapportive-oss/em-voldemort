module EM::Voldemort
  # For a given request, the router figures out which partition on which node in the cluster it
  # should be sent to. Ruby port of voldemort.routing.ConsistentRoutingStrategy.
  class Router

    def initialize(type, replicas)
      raise ClientError, "unsupported routing strategy: #{type}" if !type && type != 'consistent-routing'
      raise ClientError, "bad number of replicas: #{replicas.inspect}" if !replicas || replicas <= 0
      @replicas = replicas
    end

    # Returns a list of partitions on which a particular key can be found.
    #
    # @param key A binary string
    # @param partitions Hash of partition number to node
    # @returns Array of partitions (numbers between 0 and partitions.size - 1)
    def partitions(key, partitions)
      master = fnv_hash(key) % partitions.size
      selected = [master]
      nodes = [partitions[master]]
      current = (master + 1) % partitions.size

      # Walk clockwise around the ring of partitions, starting from the master partition.
      # The next few unique nodes in ring order are the replicas.
      while current != master && selected.size < @replicas
        if !nodes.include? partitions[current]
          nodes << partitions[current]
          selected << current
        end
        current = (current + 1) % partitions.size
      end

      selected
    end


    private

    FNV_BASIS = 0x811c9dc5
    FNV_PRIME = (1 << 24) + 0x193

    # Port of voldemort.utils.FnvHashFunction. See also http://www.isthe.com/chongo/tech/comp/fnv
    # Returns a number between 0 and 2**31 - 1.
    def fnv_hash(bytes)
      hash = FNV_BASIS
      bytes.each_byte do |byte|
        hash = (hash ^ byte) * FNV_PRIME % 2**64
        hash -= 2**64 if hash >= 2**63 # simulate overflow of signed long
      end

      # cast signed long to signed int
      hash = hash % 2**32
      hash -= 2**32 if hash >= 2**31

      # modified absolute value, as per voldemort.routing.ConsistentRoutingStrategy.abs(int)
      hash = 2**31 - 1 if hash == -2**31
      hash = -hash if hash < 0
      hash
    end

  end
end
