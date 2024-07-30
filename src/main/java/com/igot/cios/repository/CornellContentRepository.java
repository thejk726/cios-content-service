package com.igot.cios.repository;


import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cios.entity.CornellContentEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface CornellContentRepository extends JpaRepository<CornellContentEntity, String> {

    Optional<CornellContentEntity> findByExternalId(String externalId);

    List<CornellContentEntity> findByIsActive(boolean b);

    @Query(value = "SELECT c.ciosData FROM CornellContentEntity c")
    List<JsonNode> findAllCiosData();
}
