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

    # To store last ObjectID
    config_param :id_store_file, :string, :default => nil

    # SSL connection
    config_param :ssl, :bool, :default => false

    def initialize
      super
      require 'mongo'
      require 'bson'

      @client_options = {}
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

      @last_id = @id_store_file ? get_last_id : nil
      @connection_options[:ssl] = @ssl
    end

    def start
      super

      @database = get_database
      @thread = Thread.new(&method(:run))
    end

    def shutdown
      if @id_store_file
        save_last_id
        @file.close
      end

      @stop = true
      @thread.join
      @client.close

      super
    end

    def run
      loop {
        cursor = @database.find
        begin
          loop {
            return if @stop

            cursor = @database.find
            if doc = cursor.each.next
              process_document(doc)
            else
              sleep @wait_time
            end
          }
        rescue
          # ignore StopIteration Exception
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

    def get_database
      @client = client
      @client = authenticate(@client)
      @client["#{@collection}"]
    end

    def node_string
      case
      when @database
        "#{@host}:#{@port}"
      when @url
        @url
      end
    end

    def process_document(doc)
      time = if @time_key
               t = doc.delete(@time_key)
               t.nil? ? Engine.now : t.to_i
             else
               Engine.now
             end
      tag = if @tag_key
              t = doc.delete(@tag_key)
              t.nil? ? 'mongo.missing_tag' : t
            else
              @tag
            end
      if id = doc.delete('_id')
        @last_id = id.to_s
        doc['_id_str'] = @last_id
        save_last_id if @id_store_file
      end

      # Should use MultiEventStream?
      router.emit(tag, time, doc)
    end

    def get_id_store_file
      file = File.open(@id_store_file, 'w')
      file.sync
      file
    end

    def get_last_id
      if File.exist?(@id_store_file)
        BSON::ObjectId(File.read(@id_store_file)).to_s rescue nil
      else
        nil
      end
    end

    def save_last_id
      @file.pos = 0
      @file.write(@last_id)
    end
  end
end
