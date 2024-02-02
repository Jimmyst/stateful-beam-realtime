package org.stjimmy.model;

import java.io.Serializable;
import java.util.Objects;

import com.fasterxml.jackson.databind.ObjectMapper;

public class UserState implements Serializable {
    private final Double total;
    private final Long count;

    public UserState(Double total, Long count) {
        this.total = total;
        this.count = count;
    
    }


    public Double getTotal() {
        return this.total;
    }

    public Long getCount() {
        return this.count;
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
        UserState stat = (UserState) other;
        return this.total.equals(stat.total)
                && this.count.equals(stat.count);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.total, this.count);
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

