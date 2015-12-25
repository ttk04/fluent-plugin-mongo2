module Fluent
  class MongoTailInput2 < Input
    Plugin.register_input('mongo_tail2', self)

    require 'fluent/plugin/mongo_auth'
    include MongoAuthParams
    include MongoAuth

    config_param :database, :string, default: nil
    config_param :collection, :string
    config_param :host, :string, default: 'localhost'
    config_param :port, :integer, default: 27017
    config_param :wait_time, :integer, default: 1
    config_param :url, :string, default: nil

    config_param :tag, :string, default: nil
    config_param :tag_key, :string, default: nil
    config_param :time_key, :string, default: nil
    config_param :time_format, :string, default: nil

    def initialize
      super
      require 'mongo'
      require 'bson'

      @connection_options = {}
    end

    def configure(conf)
      super

      if !@tag and !@tag_key
        raise ConfigError, "'tag' or 'tag_key' option is required on mongo_tail input"
      end

      if @database && @url
        raise ConfigError, "Both 'database' and 'url' can not be set"
      end

      if !@database && !@url
        raise ConfigError, "One of 'database' or 'url' must be specified"
      end
    end

    def start
      super

      @thread = Thread.new(&method(:run))
    end

    def shutdown
      @stop = true
      @thread.join
      @client.close

      super
    end

    def run
      loop {
        begin
          loop {
            return if @stop
          }
        end
      }

    end

    private

    def client
      @client_options[:database] = @database
      @client_options[:user] = @user if @user
      @client_options[:password] = @password if @password
      Mongo::Client.new(["#{node_string}"], @client_options)
    end

    def database_name
      case
      when @database
        @database
      when @url
        Mongo::URI.new(@url).database
      end
    end

    def node_string
      case
      when @database
        "#{@host}:#{@port}"
      when @url
        @url
      end
    end
  end
end
