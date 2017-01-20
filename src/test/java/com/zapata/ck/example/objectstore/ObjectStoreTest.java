package com.zapata.ck.example.objectstore;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ObjectStoreTest {
	
	static Instance store     = null;
	static String   tableName = "cases";

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		store               = MiniClusterFixture.getInstance();
		Connector connector = MiniClusterFixture.getConnector();
		TableOperations tableOps = connector.tableOperations();
		tableOps.create(tableName);
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		Connector connector = MiniClusterFixture.getConnector();
		TableOperations tableOps = connector.tableOperations();
		tableOps.list().forEach(s -> System.out.println(String.format("table %s", s)));
		
		MiniClusterFixture.shutdown();
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void createPerson(){
		PersonValue person = PersonValue.create("John", "Smith");
		assertEquals("First name is incorrect", "John", person.getFirstName());
		assertEquals("Last name is incorrect", "Smith", person.getLastName());
	}
	
	@Test
	public void persistPerson() throws AccumuloException, AccumuloSecurityException, IOException, InterruptedException, TableNotFoundException{
		PersonValue person = PersonValue.create("John", "Smith");
		
		Connector connector = MiniClusterFixture.getConnector();
		PersistPerson.persistPerson(connector, tableName, person);
		
//		Scanner   scanner   = connector.createScanner(tableName, new Authorizations());
//		Range     range     = new Range(String.format("%s_%s_%s", person.lastName, person.firstName, person.key));
//		scanner.setRange(range);
//		for(Entry<Key,Value> entry : scanner){
//			System.out.println(String.format("row id [%s], cf [%s], cq [%s], val [%s]", entry.getKey().getRow().toString(), entry.getKey().getColumnFamily().toString(), entry.getKey().getColumnQualifier().toString(), entry.getValue().toString()));
//		}

		{
			Scanner   scanner   = connector.createScanner(tableName, new Authorizations());
			Range     range     = new Range(String.format("%s_%s_%s", person.lastName, person.firstName, person.key));
			scanner.setRange(range);
			scanner.fetchColumn(new Text("name"), new Text("firstName"));
			for(Entry<Key, Value> entry : scanner){
				assertEquals("Column family incorrect", "name", entry.getKey().getColumnFamily().toString());
				assertEquals("Column qualifier incorrect", "firstName", entry.getKey().getColumnQualifier().toString());
				assertEquals("First name value incorrect", "John", entry.getValue().toString());
			}
		}

		{
			Scanner   scanner   = connector.createScanner(tableName, new Authorizations());
			Range     range     = new Range(String.format("%s_%s_%s", person.lastName, person.firstName, person.key));
			scanner.setRange(range);
			scanner.fetchColumn(new Text("name"), new Text("lastName"));
			for(Entry<Key, Value> entry : scanner){
				assertEquals("Column family incorrect", "name", entry.getKey().getColumnFamily().toString());
				assertEquals("Column qualifier incorrect", "lastName", entry.getKey().getColumnQualifier().toString());
				assertEquals("First name value incorrect", "Smith", entry.getValue().toString());
			}
		}
	}
	
}
