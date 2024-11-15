package com.igot.cios.entity;


import com.fasterxml.jackson.databind.JsonNode;
import com.vladmihalcea.hibernate.type.json.JsonBinaryType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.Type;
import org.hibernate.annotations.TypeDef;

import javax.persistence.*;
import java.io.Serializable;
import java.sql.Timestamp;


@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "cornell_content_entity")
@IdClass(CornellContentEntityId.class)
@TypeDef(name = "jsonb", typeClass = JsonBinaryType.class)
public class CornellContentEntity implements Serializable {
    @Id
    private String externalId;
    @Id
    private String partnerId;
    @Type(type = "jsonb")
    @Column(columnDefinition = "jsonb")
    private JsonNode ciosData;
    private Boolean isActive;
    private Timestamp createdDate;
    private Timestamp updatedDate;
    @Type(type = "jsonb")
    @Column(columnDefinition = "jsonb")
    private JsonNode sourceData;
    private String fileId;
    private String partnerCode;
}
