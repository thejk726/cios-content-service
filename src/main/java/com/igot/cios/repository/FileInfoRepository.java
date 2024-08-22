package com.igot.cios.repository;

import com.igot.cios.entity.FileInfoEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface FileInfoRepository extends JpaRepository<FileInfoEntity, String> {
}
