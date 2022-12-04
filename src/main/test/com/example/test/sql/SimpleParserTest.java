package com.example.test.sql;

import com.example.Driver;
import com.example.logical.SemanticException;
import org.junit.Test;

import java.io.IOException;
import java.util.BitSet;


public class SimpleParserTest {

    @Test
    public void testSimpleSelect() throws SemanticException, IOException {
        String command = "SELECT id, name FROM my_test";
        Driver driver = new Driver();
        driver.compile(command);
    }

    @Test
    public void testSimpleJoin() throws SemanticException, IOException {
        String command = "SELECT a.id, a.age, b.id, b.depart FROM my_test a LEFT JOIN my_test b on a.id = b.id";
        Driver driver = new Driver();
        driver.compile(command);
    }

    @Test
    public void testSimpleAgg() throws SemanticException, IOException {
        String command = "SELECT count(id) FROM my_test";
        Driver driver = new Driver();
        driver.compile(command);
    }


}
