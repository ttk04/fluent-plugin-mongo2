require "helper"

class Mongo2OutputTest < ::Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    setup_mongod
  end

  def teardown
    teardown_mongod
  end

  def collection_name
    'test'
  end

  def database_name
    'fluent_test'
  end

  def port
    27017
  end

  def default_config
    %[
      type mongo
      database #{database_name}
      collection #{collection_name}
      include_time_key true
    ]
  end

  def setup_mongod
    options = {}
    options[:database] = database_name
    @client = ::Mongo::Client.new(["localhost:#{port}"], options)
  end

  def teardown_mongod
    @client[collection_name].drop
  end

  def create_driver(conf=default_config, tag='test')
    Fluent::Test::BufferedOutputTestDriver.new(Fluent::Mongo2Output, tag).configure(conf)
  end

  def test_configure
    d = create_driver(%[
      type mongo
      database fluent_test
      collection test_collection

      capped
      capped_size 100
    ])

    assert_equal('fluent_test', d.instance.database)
    assert_equal('test_collection', d.instance.collection)
    assert_equal('localhost', d.instance.host)
    assert_equal(port, d.instance.port)
    assert_equal({capped: true, size: 100}, d.instance.collection_options)
    assert_equal({ssl: false, write: {j: false}}, d.instance.client_options)
  end

  def test_configure_with_ssl
    conf = default_config + %[
      ssl true
    ]
    d = create_driver(conf)
    expected = {
      write: {
        j: false,
      },
      ssl: true,
      ssl_cert: nil,
      ssl_key: nil,
      ssl_key_pass_phrase: nil,
      ssl_verify: false,
      ssl_ca_cert: nil,
    }
    assert_equal(expected, d.instance.client_options)
  end

  def test_configure_with_write_concern
    d = create_driver(default_config + %[
      write_concern 2
    ])

    expected = {
      w: 2,
      ssl: false,
      write: {
        j: false,
      },
    }
    assert_equal(expected, d.instance.client_options)
  end

  def test_configure_with_journaled
    d = create_driver(default_config + %[
      journaled true
    ])

    expected = {
      ssl: false,
      write: {
        j: true,
      },
    }
    assert_equal(expected, d.instance.client_options)
  end

  def test_configure_with_logger_conf
    d = create_driver(default_config + %[
      mongo_log_level fatal
    ])

    expected = "fatal"
    assert_equal(expected, d.instance.mongo_log_level)
  end

  def get_documents
    @client[collection_name].find.to_a.map {|e| e.delete('_id'); e}
  end

  def emit_documents(d)
    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    d.emit({'a' => 1}, time)
    d.emit({'a' => 2}, time)
    time
  end

  def test_format
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    d.emit({'a' => 1}, time)
    d.emit({'a' => 2}, time)
    d.expect_format([time, {'a' => 1, d.instance.time_key => time}].to_msgpack)
    d.expect_format([time, {'a' => 2, d.instance.time_key => time}].to_msgpack)
    d.run

    documents = get_documents
    assert_equal(2, documents.size)
  end

  def test_write
    d = create_driver
    t = emit_documents(d)

    d.run
    actual_documents = get_documents
    time = Time.parse("2011-01-02 13:14:15 UTC")
    expected = [{'a' => 1, d.instance.time_key => time},
                {'a' => 2, d.instance.time_key => time}]
    assert_equal(expected, actual_documents)
  end

  def test_write_at_enable_tag
    d = create_driver(default_config + %[
      include_tag_key true
      include_time_key false
    ])
    t = emit_documents(d)

    d.run
    actual_documents = get_documents
    expected = [{'a' => 1, d.instance.tag_key => 'test'},
                {'a' => 2, d.instance.tag_key => 'test'}]
    assert_equal(expected, actual_documents)
  end

  class WithAuthenticateTest < self
    def setup_mongod
      options = {}
      options[:database] = database_name
      @client = ::Mongo::Client.new(["localhost:#{port}"], options)
      @client.database.users.create('fluent', password: 'password',
                                    roles: [Mongo::Auth::Roles::READ_WRITE])
    end

    def teardown_mongod
      @client[collection_name].drop
      @client.database.users.remove('fluent')
    end

    def test_write_with_authenticate
      d = create_driver(default_config + %[
        user fluent
        password password
      ])
      t = emit_documents(d)

      d.run
      actual_documents = get_documents
      time = Time.parse("2011-01-02 13:14:15 UTC")
      expected = [{'a' => 1, d.instance.time_key => time},
                  {'a' => 2, d.instance.time_key => time}]
      assert_equal(expected, actual_documents)
    end
  end

  class MongoAuthenticateTest < self
    require 'fluent/plugin/mongo_auth'
    include ::Fluent::MongoAuth

    def setup_mongod
      options = {}
      options[:database] = database_name
      @client = ::Mongo::Client.new(["localhost:#{port}"], options)
      @client.database.users.create('fluent', password: 'password',
                                    roles: [Mongo::Auth::Roles::READ_WRITE])
    end

    def teardown_mongod
      @client[collection_name].drop
      @client.database.users.remove('fluent')
    end

    def test_authenticate
      d = create_driver(default_config + %[
        user fluent
        password password
      ])

      authenticate(@client)
    end
  end
end
