package com.zapata.ck.example.objectstore;

import java.util.UUID;

public class PersonValue {
	
	
	static public PersonValue create(String firstName, String lastName){
		String uuid = UUID.randomUUID().toString();
		return new PersonValue(uuid, firstName, lastName);
	}
	String key;
	String firstName;
	String lastName;
	public String getFirstName() {
		return firstName;
	}
	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}
	public String getLastName() {
		return lastName;
	}
	public void setLastName(String lastName) {
		this.lastName = lastName;
	}
	public String getKey() {
		return key;
	}
	private PersonValue(String firstName, String lastName) {
		super();
		this.firstName = firstName;
		this.lastName = lastName;
	}
	private PersonValue(){}
	private PersonValue(String key, String firstName, String lastName) {
		super();
		this.key = key;
		this.firstName = firstName;
		this.lastName = lastName;
	};
	
	
	

}
