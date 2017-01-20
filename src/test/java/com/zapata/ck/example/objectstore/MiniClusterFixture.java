package com.zapata.ck.example.objectstore;

import java.io.File;
import java.io.IOException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;

import com.google.common.io.Files;

public class MiniClusterFixture {

	private static Instance            instance;
	private static MiniAccumuloCluster accumulo;
	private static File                tmpDirectory;
	
	public static Instance getInstance() throws IOException, InterruptedException{
		if(instance == null){
			System.out.println("creating mini cluster fixture");
			tmpDirectory     = Files.createTempDir();
			System.out.println(String.format("Create tmp directory %s", tmpDirectory.getAbsolutePath()));
			accumulo         = new MiniAccumuloCluster(tmpDirectory, "password");
			System.out.println(String.format("Created accumulo instance"));
			accumulo.start();
			System.out.println("accumulo has started");
			instance         = new ZooKeeperInstance(accumulo.getInstanceName(), accumulo.getZooKeepers());
		}
		
		return instance;
	}
	
	public static void shutdown() throws IOException, InterruptedException{
		System.out.println("shutdown mini cluster fixture");
		if(accumulo != null){
			accumulo.stop();
			System.out.println("accumulo stopped");
		}
		accumulo  = null;
		tmpDirectory.delete();
		System.out.println(String.format("tmpDirectory %s deleted", tmpDirectory.getAbsolutePath()));
	}
	
	public static Connector getConnector() throws AccumuloException, AccumuloSecurityException, IOException, InterruptedException{
		return getInstance().getConnector("root", new PasswordToken("password"));
	}

}
