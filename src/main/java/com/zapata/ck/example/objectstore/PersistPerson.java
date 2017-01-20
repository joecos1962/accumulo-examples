package com.zapata.ck.example.objectstore;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;

public class PersistPerson {
	
	static public void persistPerson(Connector connector, String tableName, PersonValue person) throws TableNotFoundException, MutationsRejectedException{
		String rowId    = String.format("%s_%s_%s", person.lastName, person.firstName, person.key);
		Mutation m = new Mutation(rowId);
		
		{
			String colFam   = "name";
			String colQual = "firstName";
			Value  val     = new Value(person.getFirstName().getBytes());
			m.put(colFam, colQual, val);
		}
		
		{
			String colFam   = "name";
			String colQual = "lastName";
			Value  val     = new Value(person.getLastName().getBytes());
			m.put(colFam, colQual, val);
		}
		
		BatchWriterConfig config = new BatchWriterConfig();
		BatchWriter bw = connector.createBatchWriter(tableName, config);
		
		bw.addMutation(m);
		bw.flush();
		bw.close();
		
	}

}
