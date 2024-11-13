package com.igot.cios.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cios.dto.DeleteContentRequestDto;
import com.igot.cios.dto.RequestDto;
import com.igot.cios.entity.FileInfoEntity;
import com.igot.cios.exception.CiosContentException;
import com.igot.cios.service.CiosContentService;
import com.igot.cios.util.elasticsearch.dto.SearchCriteria;
import com.igot.cios.util.elasticsearch.dto.SearchResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.HashMap;
import java.util.List;

@RestController
@RequestMapping("/ciosIntegration")
@Slf4j
public class CiosContentController {
    @Autowired
    CiosContentService ciosContentService;

    @PostMapping(value = "/v1/loadContentFromExcel/{partnercode}/{partnerId}", consumes = "multipart/form-data")
    public ResponseEntity<Object> loadContentFromExcel(
            @RequestParam(value = "file") MultipartFile file,
            @PathVariable("partnercode") String partnerCode,
            @PathVariable("partnerId") String partnerId){
        try {
            ciosContentService.loadContentFromExcel(file, partnerCode, partnerId);
            return ResponseEntity.status(HttpStatus.OK).body(new HashMap<>());
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error during loading of content from excel: " + e.getMessage());
        }
    }

    @PostMapping(value = "/v1/readAllContentFromDb")
    public ResponseEntity<?> fetchContentFromDb(@RequestBody RequestDto dto) {
        try {
            return ResponseEntity.ok(ciosContentService.fetchAllContentFromSecondaryDb(dto));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error during fetching of content from db: " + e.getMessage());
        }
    }

    @PostMapping(value = "/v1/loadContentProgressFromExcel/{partnercode}", consumes = "multipart/form-data")
    public ResponseEntity<Object> loadContentProgressFromExcel(@RequestParam(value = "file") MultipartFile file, @PathVariable("partnercode") String partnerCode) {
        try {
            ciosContentService.loadContentProgressFromExcel(file, partnerCode);
            return ResponseEntity.status(HttpStatus.OK).body(new HashMap<>());
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error during loading of content from excel: " + e.getMessage());
        }
    }

    @GetMapping("/v1/file/info/{partnerId}")
    public ResponseEntity<List<FileInfoEntity>> getAllFileInfosByPartnerId(@PathVariable String partnerId) {
        List<FileInfoEntity> fileInfos = ciosContentService.getAllFileInfos(partnerId);
        if (fileInfos.isEmpty()) {
            return ResponseEntity.noContent().build();
        }
        return ResponseEntity.ok(fileInfos);
    }

    @PostMapping("/v1/deleteContent")
    public ResponseEntity<?> deleteNotPublishContent(@RequestBody DeleteContentRequestDto deleteContentRequestDto) {
        try {
            return ciosContentService.deleteNotPublishContent(deleteContentRequestDto);
        } catch (CiosContentException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(e.getMessage());
        } catch (DataAccessException dae) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Database access error: " + dae.getMessage());
        }
    }

    @GetMapping("/v1/read/content/{partnercode}/{externalid}")
    public ResponseEntity<?> readContentByExternalId(@PathVariable String partnercode, @PathVariable String externalid) {
        Object response = ciosContentService.readContentByExternalId(partnercode, externalid);
        return ResponseEntity.ok(response);
    }

    @PostMapping("/v1/search/content")
    public ResponseEntity<?> searchContent(@RequestBody SearchCriteria searchCriteria) {
        SearchResult searchResult = ciosContentService.searchContent(searchCriteria);
        return new ResponseEntity<>(searchResult, HttpStatus.OK);
    }

    @PostMapping("/v1/update/content")
    public ResponseEntity<?> updateContent(@RequestBody JsonNode jsonNode) {
        Object searchResult = ciosContentService.updateContent(jsonNode);
        return new ResponseEntity<>(searchResult, HttpStatus.OK);
    }
}
