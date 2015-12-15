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
    config_param :write_concern, :integer, default: nil
    config_param :journaled, :bool, default: false

    # SSL connection
    config_param :ssl, :bool, default: false
    config_param :ssl_cert, :string, default: nil
    config_param :ssl_key, :string, default: nil
    config_param :ssl_key_pass_phrase, :string, default: nil, secret: true
    config_param :ssl_verify, :bool, default: false
    config_param :ssl_ca_cert, :string, default: nil

    attr_reader :client_options

    def initialize
      super

      require 'mongo'
      require 'msgpack'

      @client_options = {}
      @client_options = {capped: false}
    end

    def configure(conf)
      super

      @client_options[:w] = @write_concern unless @write_concern.nil?
      @client_options[:j] = @journaled
      @client_options[:ssl] = @ssl

      if @ssl
        @client_options[:ssl_cert] = @ssl_cert
        @client_options[:ssl_key] = @ssl_key
        @client_options[:ssl_key_pass_phrase] = @ssl_key_pass_phrase
        @client_options[:ssl_verify] = @ssl_verify
        @client_options[:ssl_ca_cert] = @ssl_ca_cert
      end

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
      @client_options[:database] = @database
      @client_options[:user] = @user if @user
      @client_options[:password] = @password if @password
      Mongo::Client.new(["#{@host}:#{@port}"], @client_options)
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
