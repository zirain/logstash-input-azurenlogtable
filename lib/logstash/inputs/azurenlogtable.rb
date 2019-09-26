# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "time"
require "azure/storage"

# Generate a repeating message.
#
# This plugin is intented only as an example.

class LogStash::Inputs::Azurenlogtable < LogStash::Inputs::Base
  config_name "azurenlogtable"

  config :account_name, :validate => :string
  config :access_key, :validate => :string, :required => false # not required as the sas_token can alternatively be provided
  config :sas_token, :validate => :string, :required => false # not required as the access_key can alternatively be provided
  config :table_name, :validate => :string

  config :collection_start_time_utc, :validate => :string, :default => nil #the actual value is set in the ctor (now - data_latency_minutes - 1)
  config :idle_delay_seconds, :validate => :number, :default => 15
  config :endpoint, :validate => :string, :default => "core.windows.net"

  # Default 1 minute delay to ensure all data is published to the table before querying.
  # See issue #23 for more: https://github.com/Azure/azure-diagnostics-tools/issues/23
  config :data_latency_minutes, :validate => :number, :default => 1

  # Number of past queries to be run, so we don't miss late arriving data
  config :past_queries_count, :validate => :number, :default => 5

  TICKS_SINCE_EPOCH = Time.utc(0001, 01, 01).to_i * 10000000

  def initialize(*args)
    super(*args)
    if @collection_start_time_utc.nil?
      @collection_start_time_utc = (Time.now - ( 60 * @data_latency_minutes) - 60).iso8601
      @logger.debug("collection_start_time_utc = #{@collection_start_time_utc}")
    end
  end # initialize

  public
  def register
    user_agent = "logstash-input-azurenlogtable-0.1.0"
    
    if @sas_token.nil?
        @client = Azure::Storage::Client.create(
        :storage_account_name => @account_name,
        :storage_access_key => @access_key,
        :storage_table_host => "https://#{@account_name}.table.#{@endpoint}",
        :user_agent_prefix => user_agent)
    else
      @client = Azure::Storage::Client.create(
        :storage_account_name => @account_name,
        :storage_sas_token => @sas_token,
        :storage_table_host => "https://#{@account_name}.table.#{@endpoint}",
        :user_agent_prefix => user_agent)
    end

    @azure_table_service = @client.table_client
    @last_timestamp = partitionkey_from_datetime(@collection_start_time_utc)
    @idle_delay = @idle_delay_seconds
  end # def register

  public
  def run(output_queue)
    while !stop?
      @logger.debug("Starting process method @" + Time.now.to_s);
      process(output_queue)
      @logger.debug("Starting delay of: " + @idle_delay.to_s + " seconds @" + Time.now.to_s);
      sleep @idle_delay
    end # while
  end # run

  def process(output_queue)
    @until_timestamp = partitionkey_from_datetime(Time.now.iso8601)
    last_good_timestamp = nil

    log_count = 0

    query = build_latent_query
    query.reset
    query.run( ->(entity) {
      last_good_timestamp = on_new_data(entity, output_queue, last_good_timestamp)
      log_count += 1
    })
    
    @logger.debug("log total count => #{log_count}")
    if (!last_good_timestamp.nil?)
      @last_timestamp = last_good_timestamp
    end

  rescue => e
    @logger.error("Oh My, An error occurred. Error:#{e}: Trace: #{e.backtrace}", :exception => e)
    raise
  end # process

  def build_latent_query
    @logger.debug("from #{@last_timestamp} to #{@until_timestamp}")
    if @last_timestamp > @until_timestamp
      @logger.debug("last_timestamp is in the future. Will not run any query!")
      return nil
    end
    query_filter = "(PartitionKey gt '#{@last_timestamp}' and PartitionKey lt '#{@until_timestamp}')"
    query_filter = query_filter.gsub('"','')
    return AzureQuery.new(@logger, @azure_table_service, @table_name, query_filter, @last_timestamp.to_s + "-" + @until_timestamp.to_s, @entity_count_to_process)
  end

  def on_new_data(entity, output_queue, last_good_timestamp)
    #@logger.debug("new event")
    
    event = LogStash::Event.new(entity.properties)
    event.set("type", @table_name)
    @logger.debug("new event:" + event.to_hash.to_s)

    decorate(event)
    last_good_timestamp = event.get('PartitionKey')
    output_queue << event
    return last_good_timestamp
  end

  # Windows Azure Diagnostic's algorithm for determining the partition key based on time is as follows:
  # 1. Take time in UTC without seconds.
  # 2. Convert it into .net ticks
  # 3. add a '0' prefix.
  def partitionkey_from_datetime(time_string)
    if time_string.nil?
      @logger.warn("partitionkey_from_datetime with invalid time_string. ")
      collection_time = (Time.now - ( 60 * @data_latency_minutes) - 60)
    else
      begin
        collection_time = Time.parse(time_string)
      rescue => e  
        @logger.error("partitionkey_from_datetime fail with time_string =>" + time_string, :exception => e)
        collection_time = (Time.now - ( 60 * @data_latency_minutes) - 60)
      end
    end
    if collection_time
      #@logger.debug("collection time parsed successfully #{collection_time}")
    else
      raise(ArgumentError, "Could not parse the time_string => #{time_string}")
    end # if else block

    collection_time -= collection_time.sec
    ticks = to_ticks(collection_time)
    "0#{ticks}"
  end # partitionkey_from_datetime

  # Convert time to ticks
  def to_ticks(time_to_convert)
    #@logger.debug("Converting time to ticks")
    time_to_convert.to_i * 10000000 - TICKS_SINCE_EPOCH
  end # to_ticks

  def stop
    # nothing to do in this case so it is not necessary to define stop
    # examples of common "stop" tasks:
    #  * close sockets (unblocking blocking reads/accepts)
    #  * cleanup temporary files
    #  * terminate spawned threads
  end
end # class LogStash::Inputs::Azurenlogtable

class AzureQuery
  def initialize(logger, azure_table_service, table_name, query_str, query_id, entity_count_to_process)
    @logger = logger
    @query_str = query_str
    @query_id = query_id
    @entity_count_to_process = entity_count_to_process
    @azure_table_service = azure_table_service
    @table_name = table_name
    @continuation_token = nil
  end

  def reset
    @continuation_token = nil
  end

  def id
    return @query_id
  end

  def run(on_result_cbk)
    results_found = false
    @logger.debug("[#{@query_id}]Query filter: " + @query_str)
    begin
      @logger.debug("[#{@query_id}]Running query. continuation_token: #{@continuation_token}")
      query = { :top => @entity_count_to_process, :filter => @query_str, :continuation_token => @continuation_token }
      result = @azure_table_service.query_entities(@table_name, query)

      if result and result.length > 0
        results_found = true
        @logger.debug("[#{@query_id}] #{result.length} results found.")
        result.each do |entity|
          on_result_cbk.call(entity)
        end
      end

      @continuation_token = result.continuation_token
    end until !@continuation_token

    return results_found
  end
end # class AzureQuery