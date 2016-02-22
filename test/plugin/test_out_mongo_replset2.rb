require "helper"

class MongoReplset2OutputTest < ::Test::Unit::TestCase
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
      replica_set database_name
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
    Fluent::Test::BufferedOutputTestDriver.new(Fluent::MongoOutputReplset2, tag).configure(conf)
  end

  def test_configure
    d = create_driver(%[
      type mongo
      database fluent_test
      collection test_collection

      replica_set test
    ])

    assert_equal('fluent_test', d.instance.database)
    assert_equal('test_collection', d.instance.collection)
    assert_equal('localhost', d.instance.host)
    assert_equal(port, d.instance.port)
    assert_equal({replica_set: 'test', :ssl=>false, :write=>{:j=>false}},
                 d.instance.client_options)
  end
end
