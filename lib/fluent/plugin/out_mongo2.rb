module Fluent
  class Mongo2Output < BufferedOutput
    Plugin.register_output('mongo2', self)

    include SetTagKeyMixin
    config_set_default :include_tag_key, false

    include SetTimeKeyMixin
    config_set_default :include_time_key, true

    config_param :database, :string
    config_param :collection, :string, default: 'untagged'
    config_param :host, :string, default: 'localhost'
    config_param :port, :integer, default: 27017

    def initialize
      super

      require 'mongo'
      require 'msgpack'
    end

    def configure(conf)
      super

      # MongoDB uses BSON's Date for time.
      def @timef.format_nocache(time)
        time
      end
    end

    def start
      @client = client
      super
    end

    def shutdown
      @client.close
      super
    end

    def emit(tag, es, chain)
      super(tag, es, chain)
    end

    def format(tag, time, record)
      [time, record].to_msgpack
    end

    def write(chunk)
      operate(@client, collect_records(chunk))
    end

    private

    def client
      options = {}
      options[:database] = @database
      options[:user] = @user if @user
      options[:password] = @password if @password
      Mongo::Client.new(["#{@host}:#{@port}"], options)
    end

    def collect_records(chunk)
      records = []
      chunk.msgpack_each {|time, record|
        record[@time_key] = Time.at(time || record[@time_key]) if @include_time_key
        records << record
      }
      records
    end

    def operate(client, records)
      begin
        result = client[@collection].insert_many(records)
      rescue => e
        puts e
      end
      records
    end
  end
end
