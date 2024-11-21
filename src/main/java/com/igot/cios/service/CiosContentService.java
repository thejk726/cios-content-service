package com.igot.cios.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cios.dto.DeleteContentRequestDto;
import com.igot.cios.dto.PaginatedResponse;
import com.igot.cios.dto.RequestDto;
import com.igot.cios.dto.SBApiResponse;
import com.igot.cios.entity.FileInfoEntity;
import com.igot.cios.util.elasticsearch.dto.SearchCriteria;
import com.igot.cios.util.elasticsearch.dto.SearchResult;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.List;


@Service
public interface CiosContentService {
    SBApiResponse loadContentFromExcel(MultipartFile file, String partnerCode, String partnerId) throws IOException;

    PaginatedResponse<?> fetchAllContentFromSecondaryDb(RequestDto dto);

    void loadContentProgressFromExcel(MultipartFile file, String orgId);

    List<FileInfoEntity> getAllFileInfos(String partnerId);

    ResponseEntity<?> deleteNotPublishContent(DeleteContentRequestDto deleteContentRequestDto);

    Object readContentByExternalId(String partnercode, String externalid);

    SearchResult searchContent(SearchCriteria searchCriteria);

    Object updateContent(JsonNode jsonNode);
}
