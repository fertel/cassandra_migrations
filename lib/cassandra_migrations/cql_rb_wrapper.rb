require 'cassandra'

module Cassandra
  class PreparedStatement
    attr_reader :statement

    def initialize(client, statement)
      @client = client
      @statement = statement
    end

    def execute(*args)
      @client.execute(@statement, *args)
    end
  end

  class BatchStatement
    def initialize(client, batch)
      @client = client
      @batch = batch
    end

    def execute(options = {})
      @client.execute(@batch, options)
    end

    def add(*args)
      @batch.add(*args)
      self
    end
  end

  module Client
    class Client
      def initialize(session)
        @session = session
      end

      def execute(*args)
        @session.execute(*args)
      end

      def prepare(statement, options = {})
        s = @session.prepare(statement, options)
        PreparedStatement.new(self, s)
      end

      def batch(type = :logged, options = {})
        batch = BatchStatement.new(self, @session.send(:"#{type}_batch"))
        if block_given?
          yield(batch)
          batch.execute(options)
        else
          batch
        end
      end

      def close
        @session.close
      end
    end
  end
end

Cql = Cassandra
