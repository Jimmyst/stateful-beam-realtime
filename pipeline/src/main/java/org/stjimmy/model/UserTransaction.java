package org.stjimmy.model;

import java.io.Serializable;
import java.util.Objects;

import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

public class UserTransaction implements Serializable {
    private final String uid;
    private final String item;
    private final Double price;
    private final String transaction_id;
    private final Long transaction_ts;

    public UserTransaction(String uid, String item, Double price, String transaction_id, Long transaction_ts) {
        this.uid = uid;
        this.item = item;
        this.price = price;
        this.transaction_id = transaction_id;
        this.transaction_ts = transaction_ts;
    }


    public String getUid() {
        return this.uid;
    }

    public String getItem() {
        return this.item;
    }

    public Double getPrice() {
        return this.price;
    }

    @JsonProperty("transaction_id")
    public String getTransactionId() {
        return this.transaction_id;
    }

    @JsonProperty("transaction_ts")
    public Long getTransactionTs() {
        return this.transaction_ts;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null) {
            return false;
        }
        if (getClass() != other.getClass()) {
            return false;
        }
        UserTransaction transaction = (UserTransaction) other;
        return this.uid.equals(transaction.uid)
                && this.item.equals(transaction.item)
                && this.price.equals(transaction.price)
                && this.transaction_id.equals(transaction.transaction_id)
                && this.transaction_ts.equals(transaction.transaction_ts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.uid, this.item, this.price, this.transaction_id, this.transaction_ts);
    }

    public String toJosn() {
        String json = "";
        ObjectMapper mapper = new ObjectMapper();
        try {
            json = mapper.writeValueAsString(this);
        } catch (Exception e) {

        }
        return json;
    }
}

