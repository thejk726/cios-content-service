package com.igot.cios.service;


import com.igot.cios.dto.PaginatedResponse;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;


@Service
public interface CiosContentService {
    void loadContentFromExcel(MultipartFile file, String name) throws IOException;

    PaginatedResponse<Object> fetchAllContentFromDb(String providerName, Boolean isActive, int page, int size);

    void loadContentProgressFromExcel(MultipartFile file);
}
