package com.igot.cios.service;


import com.igot.cios.entity.CornellContentEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.List;

@Service
public interface CiosContentService {
    void loadContentFromExcel(MultipartFile file,String name) throws IOException;
    List<CornellContentEntity> fetchAllContentFromDb();

    void loadContentProgressFromExcel(MultipartFile file);
}
