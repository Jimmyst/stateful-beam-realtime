package org.stjimmy.model;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class UserTransactionTest {



     @Test
  public void testTransactionToJson() throws Exception {
    UserTransaction transaction = new UserTransaction("uid", "item", 10.00, "t_id", Long.valueOf(12345));    
    assertEquals(transaction.toJosn() ,"{\"uid\":\"uid\",\"item\":\"item\",\"price\":10.0,\"transaction_id\":\"t_id\",\"transaction_ts\":12345}");

  }

    
}
