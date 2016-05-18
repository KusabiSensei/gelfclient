package org.graylog2.gelfclient.transport;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.rabbitmq.client.*;
import org.graylog2.gelfclient.GelfConfiguration;
import org.graylog2.gelfclient.GelfMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GelfAMQPTransport implements GelfTransport {
	private static final Logger LOG = LoggerFactory.getLogger(GelfAMQPTransport.class);
	private ConnectionFactory factory;
	private Connection rabbitMQConnection;
	private Channel rabbitMQChannel;
	protected final GelfConfiguration config;
    protected final BlockingQueue<GelfMessage> queue;
	
	public GelfAMQPTransport(GelfConfiguration config, BlockingQueue<GelfMessage> queue) {
		this.config = config;
		this.queue = queue;
		startup();
	}

	public GelfAMQPTransport(GelfConfiguration config) {
		this.config = config;
		this.queue = new LinkedBlockingQueue<GelfMessage>(config.getQueueSize());
		startup();
	}

	private void startup() {
		//Startup the factory and set the options
		factory = new ConnectionFactory();
		//Set this to what it should be
		//TODO: Build the URI or options from config
		try {
			factory.setUri("amqp://username:password@host:port/virtualHost");
			rabbitMQConnection = factory.newConnection();
			rabbitMQChannel = rabbitMQConnection.createChannel();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
		
	@Override
	public void send(GelfMessage message) throws InterruptedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean trySend(GelfMessage message) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void stop() {
		// Stop the RabbitMQ Channel and Connection
		try {
			rabbitMQChannel.close();
			rabbitMQConnection.close();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
	}



}
