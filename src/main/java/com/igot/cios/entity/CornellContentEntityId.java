package com.igot.cios.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class CornellContentEntityId implements Serializable {
    private String externalId;
    private String partnerId;

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        CornellContentEntityId that = (CornellContentEntityId) obj;
        return externalId.equals(that.externalId) && partnerId.equals(that.partnerId);
    }

    @Override
    public int hashCode() {
        return 31 * externalId.hashCode() + partnerId.hashCode();
    }
}

