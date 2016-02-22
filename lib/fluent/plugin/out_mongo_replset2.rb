require 'fluent/plugin/out_mongo2'

module Fluent
  class MongoOutputReplset2 < Mongo2Output
    Plugin.register_output('mongo_replset2', self)

    config_set_default :include_tag_key, false
    config_set_default :include_time_key, true

    config_param :replica_set, :string
    config_param :read, :string, :default => nil
    config_param :num_retries, :integer, :default => 60

    def configure(conf)
      super

      if replica_set = conf['replica_set']
        @client_options[:replica_set] = replica_set
      end
      if read = conf['read']
        @client_options[:read] = read.to_sym
      end

      $log.debug "Setup replica set configuration: #{conf['replica_set']}"
    end

    private

    def operate(client, records)
      rescue_connection_failure do
        super(client, records)
      end
    end

    def rescue_connection_failure
      retries = 0
      begin
        yield
      rescue Mongo::Error::OperationFailure => e
        retries += 1
        raise e if retries > @num_retries

        log.warn "Failed to operate to Replica Set. Try to retry: retry count = #{retries}"

        sleep 0.5
        retry
      end
    end
  end
end
