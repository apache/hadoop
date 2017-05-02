package com.junit.junitejemplo;

import static org.junit.Assert.*;

import org.junit.Test;

public class jTest {

	Charfun helper = new Charfun();
	@Test
	public void test0() {
		//test simple compara luego de borrar las dos A en la funcion1
		assertEquals("CD",helper.funcion1("AACD"));
	}
	// expected:<ABC[]> but was:<ABC[D]>
	@Test
	public void test1(){
		assertEquals("CD",helper.funcion1("ACD"));
	}
	@Test
	//compara si las dos primeras letras son iguales a las dos ultimas
	public void test2(){
		boolean actualValue = helper.funcion2("ABAB");
		assertTrue(actualValue);
		
	}
	@Test
	//assert false comprueba que en la funcion2 no paso 
	public void test3(){
		boolean actualValue=helper.funcion2("ABCD");
		assertFalse(actualValue);
	}
	@Test
	public void test4(){
		boolean val=false;
		assertFalse(helper.funcion1("ABA"),val);
		
	}
}



