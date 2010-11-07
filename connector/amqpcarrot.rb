require 'carrot'
require 'time'

module MCollective
    module Connector
        # Handles sending and receiving messages over the AMQP protocol
        #
        # This plugin supports version 0.8.1 of the Carrot rubygem
        #
        # You can configure it as follows:
        #
        #    connector = amqpcarrot
        #    plugin.amqpcarrot.host = amqpbroker.your.net
        #    plugin.amqpcarrot.port = 5762
        #    plugin.amqpcarrot.user = you
        #    plugin.amqpcarrot.password = secret
        #
        # All of these can be overriden per user using environment variables:
        #
        #    AMQP_SERVER, AMQP_PORT, AMQP_USER, AMQP_PASSWORD
        #
        class AMQPCarrot<Base
            attr_reader :connection

            def initialize
                @config = Config.instance
                @subscriptions = []

                @log = Log.instance
            end

            # Connects to the AMQP middleware
            def connect
                if @connection
                    @log.debug("Already connection, not re-initializing connection")
                    return
                end

                begin
                    host = nil
                    port = nil
                    user = nil
                    password = nil

                    host = get_env_or_option("AMQP_SERVER", "amqpcarrot.host")
                    port = get_env_or_option("AMQP_PORT", "amqpcarrot.port", 5762).to_i
                    user = get_env_or_option("AMQP_USER", "amqpcarrot.user")
                    password = get_env_or_option("AMQP_PASSWORD", "amqpcarrot.password")

                    @log.debug("Connecting to #{host}:#{port}")
                    @connection = ::Carrot.new(:host => host, :port => port, :user=>user, :pass=>password)
                    @exchange = @connection.topic
                    @queue = @connection.queue(nil, :nowait => false)
                rescue Exception => e
                    raise("Could not connect to AMQP broker: #{e}")
                end
            end

            # Receives a message from the AMQP queue
            def receive
                @log.debug("Waiting for a message from queue")
                msg = nil
                while msg.nil? do
                    # FIXME - a blocking read would be *really* nice
                    msg = @queue.pop :ack => true
                    sleep 0.25
                end

                @queue.ack

                Request.new(msg)
            end

            # Sends a message to the AMQP exchange with the target topic
            def send(target, msg)
                @log.debug("Sending a message with routing key '#{target}'")
                @exchange.publish(msg, :key => target)
            end

            # Subscribe to a topic or queue
            def subscribe(source)
                unless @subscriptions.include?(source)
                    @log.debug("Subscribing to #{source}")
                    @queue.bind(@exchange, :routing_key => source)
                    @subscriptions << source
                end
            end

            # Unsubscribe from a topic or queue
            def unsubscribe(source)
                @log.debug("Unsubscribing from #{source}")
                @queue.unbind(@exchange, :routing_key => source)
                @subscriptions.delete(source)
            end

            # Disconnects from the AMQP broker
            def disconnect
                @log.debug("Disconnecting from AMQP broker")
                @connection.stop
            end

            private
            # looks in the environment first then in the config file
            # for a specific option, accepts an optional default.
            #
            # raises an exception when it cant find a value anywhere
            def get_env_or_option(env, opt, default=nil)
                return ENV[env] if ENV.include?(env)
                return @config.pluginconf[opt] if @config.pluginconf.include?(opt)
                return default if default

                raise("No #{env} environment or plugin.#{opt} configuration option given")
            end

            # looks for a config option, accepts an optional default
            #
            # raises an exception when it cant find a value anywhere
            def get_option(opt, default=nil)
                return @config.pluginconf[opt] if @config.pluginconf.include?(opt)
                return default if default

                raise("No plugin.#{opt} configuration option given")
            end

            # gets a boolean option from the config, supports y/n/true/false/1/0
            def get_bool_option(opt, default)
                return default unless @config.pluginconf.include?(opt)

                val = @config.pluginconf[opt]

                if val =~ /^1|yes|true/
                    return true
                elsif val =~ /^0|no|false/
                    return false
                else
                    return default
                end
            end
        end
    end
end

# vi:tabstop=4:expandtab:ai
