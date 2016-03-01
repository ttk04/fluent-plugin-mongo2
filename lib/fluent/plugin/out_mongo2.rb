module Fluent
  class Mongo2Output < BufferedOutput
    Plugin.register_output('mongo2', self)

    require 'fluent/plugin/mongo_auth'
    include MongoAuthParams
    include MongoAuth
    require 'fluent/plugin/logger_support'
    include LoggerSupport

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
    config_param :replace_dot_in_key_with, :string, default: nil
    config_param :replace_dollar_in_key_with, :string, default: nil

    # tag mapping mode
    config_param :tag_mapped, :bool, default: false
    config_param :remove_tag_prefix, :string, default: nil

    # SSL connection
    config_param :ssl, :bool, default: false
    config_param :ssl_cert, :string, default: nil
    config_param :ssl_key, :string, default: nil
    config_param :ssl_key_pass_phrase, :string, default: nil, secret: true
    config_param :ssl_verify, :bool, default: false
    config_param :ssl_ca_cert, :string, default: nil

    attr_reader :client_options, :collection_options

    def initialize
      super

      require 'mongo'
      require 'msgpack'

      @client_options = {}
      @collection_options = {capped: false}
    end

    def configure(conf)
      super

      if conf.has_key?('tag_mapped')
        @tag_mapped = true
      end
      raise ConfigError, "normal mode requires collection parameter" if !@tag_mapped and !conf.has_key?('collection')

      if conf.has_key?('capped')
        raise ConfigError, "'capped_size' parameter is required on <store> of Mongo output" unless conf.has_key?('capped_size')
        @collection_options[:capped] = true
        @collection_options[:size] = Config.size_value(conf['capped_size'])
        @collection_options[:max] = Config.size_value(conf['capped_max']) if conf.has_key?('capped_max')
      end

      if remove_tag_prefix = conf['remove_tag_prefix']
        @remove_tag_prefix = Regexp.new('^' + Regexp.escape(remove_tag_prefix))
      end

      @client_options[:w] = @write_concern unless @write_concern.nil?
      @client_options[:write] = {j: @journaled}
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

      configure_logger(@mongo_log_level)

      $log.debug "Setup mongo configuration: mode = #{@tag_mapped ? 'tag mapped' : 'normal'}"
    end

    def start
      @client = client
      @client = authenticate(@client)
      super
    end

    def shutdown
      @client.close
      super
    end

    def emit(tag, es, chain)
      if @tag_mapped
        super(tag, es, chain, tag)
      else
        super(tag, es, chain)
      end
    end

    def format(tag, time, record)
      [time, record].to_msgpack
    end

    def write(chunk)
      collection_name = @tag_mapped ? chunk.key : @collection
      operate(format_collection_name(collection_name), collect_records(chunk))
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

    FORMAT_COLLECTION_NAME_RE = /(^\.+)|(\.+$)/

    def format_collection_name(collection_name)
      formatted = collection_name
      formatted = formatted.gsub(@remove_tag_prefix, '') if @remove_tag_prefix
      formatted = formatted.gsub(FORMAT_COLLECTION_NAME_RE, '')
      formatted = @collection if formatted.size == 0 # set default for nil tag
      formatted
    end

    def operate(collection, records)
      begin
        if @replace_dot_in_key_with
          records.map! do |r|
            replace_key_of_hash(r, ".", @replace_dot_in_key_with)
          end
        end
        if @replace_dollar_in_key_with
          records.map! do |r|
            replace_key_of_hash(r, /^\$/, @replace_dollar_in_key_with)
          end
        end

        @client[collection, @collection_options].insert_many(records)
      rescue Mongo::Error::BulkWriteError => e
        log.warn e
      rescue ArgumentError => e
        log.warn e
      end
      records
    end

    # Copied from https://github.com/fluent/fluent-plugin-mongo/blob/82106b8d8551b29d503bca0ff5f9927eca0de0e0/lib/fluent/plugin/out_mongo.rb#L274
    def replace_key_of_hash(hash_or_array, pattern, replacement)
      case hash_or_array
      when Array
        hash_or_array.map do |elm|
          replace_key_of_hash(elm, pattern, replacement)
        end
      when Hash
        result = Hash.new
        hash_or_array.each_pair do |k, v|
          k = k.gsub(pattern, replacement)

          if v.is_a?(Hash) || v.is_a?(Array)
            result[k] = replace_key_of_hash(v, pattern, replacement)
          else
            result[k] = v
          end
        end
        result
      end
    end
  end
end
