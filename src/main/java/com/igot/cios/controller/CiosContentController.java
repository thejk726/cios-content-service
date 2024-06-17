package com.igot.cios.controller;

import com.igot.cios.entity.ExternalContentEntity;
import com.igot.cios.service.CiosContentService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;

@RestController
@RequestMapping("/cios-integration")
@Slf4j
public class CiosContentController {
    @Autowired
    CiosContentService ciosContentService;

    @PostMapping(value = "/v1/loadContentFromExcel", consumes = "multipart/form-data")
    public ResponseEntity<String> loadJobsFromExcel(@RequestParam(value = "file") MultipartFile file) {
        try {
            ciosContentService.loadContentFromExcel(file);
            return ResponseEntity.ok("Loading of content from excel is successful.");
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body("Error during loading of content from excel: " + e.getMessage());
        }
    }

    @GetMapping(value = "/v1/readAllContentFromDb")
    public ResponseEntity<?> fetchContentFromDb() {
        try {
            return ResponseEntity.ok(ciosContentService.fetchAllContentFromDb());
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error during loading of content from excel: " + e.getMessage());
        }
    }
}
