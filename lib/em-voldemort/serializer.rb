module EM::Voldemort
  # Translates between raw bytes in the store and structured objects, based on a schema in the store
  # metadata
  class Serializer

    attr_reader :type, :schemas

    def initialize(xml)
      @type = xml.xpath('type').text
      @schemas = xml.xpath('schema-info').each_with_object({}) do |schema, hash|
        hash[schema['version'].to_i] = schema.text
      end
    end

    def encode(data)
      data # TODO
    end

    def decode(data)
      data # TODO
    end
  end
end
