package com.igot.cios.repository;

import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cios.entity.UpgradContentEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface UpgradContentRepository extends JpaRepository<UpgradContentEntity, String> {

    Optional<UpgradContentEntity> findByExternalId(String externalId);

    List<UpgradContentEntity> findByIsActive(boolean b);

    @Query(value = "SELECT c.ciosData FROM UpgradContentEntity c")
    List<JsonNode> findAllCiosData();
}
