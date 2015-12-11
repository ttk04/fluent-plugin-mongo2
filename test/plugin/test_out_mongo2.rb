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
    ])

    assert_equal('fluent_test', d.instance.database)
    assert_equal('test_collection', d.instance.collection)
    assert_equal('localhost', d.instance.host)
    assert_equal(port, d.instance.port)
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
end
