package org.stjimmy.parser;
import org.stjimmy.model.UserTransaction;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class UserTransactionParser {


    public static UserTransaction parseJsonToTransactioin(String json) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(json);
        try {
            UserTransaction res = new UserTransaction(
                root.get("uid").textValue(),
                root.get("item").textValue(),
                root.get("price").doubleValue(),
                root.get("transaction_id").textValue(),
                root.get("transaction_ts").longValue()
            );
            return res;
        } catch (Exception e) {
            System.err.println("Seriialisation error: "+ e.getMessage());
            return null;
        }
        
        
    }

}