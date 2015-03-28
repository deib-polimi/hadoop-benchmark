package it.polimi.hadoop.hadoopnn;

import it.polimi.hadoop.hadoopnn.ec2.VirtualMachine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
	
	private static final Logger logger = LoggerFactory.getLogger(Main.class);

	public static void main(String[] args) {
		VirtualMachine.createSecurityGroup();
		
		try {
			logger.info("Initializing the environment...");
			
			VirtualMachine slave = VirtualMachine.getVM("slave");
			VirtualMachine master = VirtualMachine.getVM("master");
		
			logger.info("Starting the system...");
			
			slave.spotRequest();
			master.spotRequest();
			
			boolean running = slave.waitUntilRunning();
			running = running && master.waitUntilRunning();
			
			if (running)
				logger.info("System running!");
			else
				logger.error("There were some errors!");
			
			try {
				Thread.sleep(60*1000);
			} catch (InterruptedException e) {
				logger.error("Error while waiting.", e);
			}
			
			slave.terminateAllSpots();
			master.terminateAllSpots();
			
			logger.info("System shutted down!");
		} catch (Exception e) {
			logger.error("Error found while running!", e);
		}
	}

}
