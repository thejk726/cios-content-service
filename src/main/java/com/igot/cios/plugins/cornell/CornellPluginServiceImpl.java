package com.igot.cios.plugins.cornell;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cios.dto.DeleteContentRequestDto;
import com.igot.cios.dto.RequestDto;
import com.igot.cios.dto.SBApiResponse;
import com.igot.cios.entity.CornellContentEntity;
import com.igot.cios.exception.CiosContentException;
import com.igot.cios.plugins.ContentPartnerPluginService;
import com.igot.cios.plugins.DataTransformUtility;
import com.igot.cios.repository.CornellContentRepository;
import com.igot.cios.util.CbServerProperties;
import com.igot.cios.util.Constants;
import com.igot.cios.util.elasticsearch.service.EsUtilService;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Service
public class CornellPluginServiceImpl implements ContentPartnerPluginService {

    @Autowired
    private CornellContentRepository cornellContentRepository;

    @Autowired
    DataTransformUtility dataTransformUtility;

    @Autowired
    private EsUtilService esUtilService;
    @Autowired
    private CbServerProperties cbServerProperties;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private RestHighLevelClient client;


    @Override
    public void loadContentFromExcel(JsonNode processedData, String partnerCode, String fileName, String fileId, List<Object> contentJson) {
        List<CornellContentEntity> cornellContentEntityList = new ArrayList<>();
        processedData.forEach(eachContentData -> {
            JsonNode transformData = dataTransformUtility.transformData(eachContentData, contentJson);
            Timestamp currentTime = new Timestamp(System.currentTimeMillis());
            ((ObjectNode) transformData.path(Constants.CONTENT)).put(Constants.FILE_ID, fileId).asText();
            ((ObjectNode) transformData.path(Constants.CONTENT)).put(Constants.SOURCE, fileName).asText();
            ((ObjectNode) transformData.path(Constants.CONTENT)).put(Constants.PARTNER_CODE, partnerCode).asText();
            ((ObjectNode) transformData.path(Constants.CONTENT)).put(Constants.STATUS, Constants.NOT_INITIATED).asText();
            ((ObjectNode) transformData.path(Constants.CONTENT)).put(Constants.CREATED_DATE, currentTime.toString()).asText();
            ((ObjectNode) transformData.path(Constants.CONTENT)).put(Constants.UPDATED_DATE, currentTime.toString()).asText();
            ((ObjectNode) transformData.path(Constants.CONTENT)).put(Constants.ACTIVE, Constants.ACTIVE_STATUS).asText();
            ((ObjectNode) transformData.path(Constants.CONTENT)).put(Constants.PUBLISHED_ON, "0000-00-00 00:00:00").asText();
            dataTransformUtility.validatePayload(Constants.DATA_PAYLOAD_VALIDATION_FILE, transformData);
            addSearchTags(transformData);
            String externalId = transformData.path(Constants.CONTENT).path(Constants.EXTERNAL_ID).asText();
            CornellContentEntity cornellContentEntity = saveOrUpdateCornellContent(externalId, transformData, eachContentData, currentTime, fileId);
            cornellContentEntityList.add(cornellContentEntity);

        });
        cornellBulkSave(cornellContentEntityList, partnerCode);
    }

    private JsonNode addSearchTags(JsonNode transformData) {
        List<String> searchTags = new ArrayList<>();
        searchTags.add(transformData.path(Constants.CONTENT).get(Constants.NAME).textValue().toLowerCase());
        ArrayNode searchTagsArray = objectMapper.valueToTree(searchTags);
        ((ObjectNode) transformData.path(Constants.CONTENT)).put(Constants.CONTENT_SEARCH_TAGS, searchTagsArray);
        return transformData;
    }

    private CornellContentEntity saveOrUpdateCornellContent(String externalId, JsonNode transformData, JsonNode rawContentData, Timestamp currentTime, String fileId) {
        Optional<CornellContentEntity> optExternalContent = cornellContentRepository.findByExternalId(externalId);
        if (optExternalContent.isPresent()) {
            CornellContentEntity externalContent = optExternalContent.get();
            externalContent.setExternalId(externalId);
            externalContent.setCiosData(transformData);
            externalContent.setIsActive(externalContent.getIsActive());
            externalContent.setCreatedDate(externalContent.getCreatedDate());
            externalContent.setUpdatedDate(currentTime);
            externalContent.setSourceData(rawContentData);
            externalContent.setFileId(fileId);
            return externalContent;
        } else {
            CornellContentEntity externalContent = new CornellContentEntity();
            externalContent.setExternalId(externalId);
            externalContent.setCiosData(transformData);
            externalContent.setIsActive(false);
            externalContent.setCreatedDate(currentTime);
            externalContent.setUpdatedDate(currentTime);
            externalContent.setSourceData(rawContentData);
            externalContent.setFileId(fileId);
            return externalContent;
        }

    }

    private void cornellBulkSave(List<CornellContentEntity> cornellContentEntityList, String partnerCode) {
        cornellContentRepository.saveAll(cornellContentEntityList);
        cornellContentEntityList.forEach(contentEntity -> {
            try {
                Map<String, Object> entityMap = objectMapper.convertValue(contentEntity, Map.class);
                flattenContentData(entityMap);
                String uniqueId = partnerCode + "_" + contentEntity.getExternalId();
                esUtilService.addDocument(
                        Constants.CIOS_CONTENT_INDEX_NAME,
                        Constants.INDEX_TYPE,
                        uniqueId,
                        entityMap,
                        cbServerProperties.getElasticCiosContentJsonPath()
                );
                log.info("Added data to ES document for externalId: {}", contentEntity.getExternalId());
            } catch (Exception e) {
                log.error("Error while processing contentEntity with externalId: {}", contentEntity.getExternalId(), e);
            }
        });
        Long totalCourseCount = cornellContentRepository.count();
        JsonNode response = dataTransformUtility.fetchPartnerInfoUsingApi(partnerCode);
        JsonNode resultData = response.path(Constants.RESULT);
        JsonNode data = resultData.path(Constants.DATA);
        ((ObjectNode) data).put(Constants.TOTAL_COURSES_COUNT, totalCourseCount);
        dataTransformUtility.updatingPartnerInfo(resultData);
    }

    private void flattenContentData(Map<String, Object> entityMap) {
        if (entityMap.containsKey("ciosData") && entityMap.get("ciosData") instanceof Map) {
            Map<String, Object> ciosDataMap = (Map<String, Object>) entityMap.get("ciosData");
            if (ciosDataMap.containsKey("content") && ciosDataMap.get("content") instanceof Map) {
                Map<String, Object> contentMap = (Map<String, Object>) ciosDataMap.get("content");
                entityMap.putAll(contentMap);
                entityMap.remove(Constants.CIOS_DATA);
                entityMap.remove(Constants.SOURCE_DATA);
            }
        }
    }


    @Override
    public Page<?> fetchAllContentFromSecondaryDb(RequestDto dto) {
        Pageable pageable = PageRequest.of(dto.getPage(), dto.getSize());
        return cornellContentRepository.findAllCiosDataAndIsActive(dto.getIsActive(), pageable, dto.getKeyword());
    }

    @Override
    public Object readContentByExternalId(String externalid) {
        Optional<CornellContentEntity> entity = cornellContentRepository.findByExternalId(externalid);
        if (entity.isPresent()) {
            return entity.get().getCiosData();
        } else {
            throw new CiosContentException("No data found for given id", externalid, HttpStatus.BAD_REQUEST);
        }

    }

    @Override
    public Object updateContent(JsonNode jsonNode, String partnerCode) {
        Timestamp currentTime = new Timestamp(System.currentTimeMillis());
        String externalId = jsonNode.path("content").get("externalId").asText();
        Boolean isActive = jsonNode.path("content").get("isActive").asBoolean(false);
        Optional<CornellContentEntity> optExternalContent = cornellContentRepository.findByExternalId(externalId);
        if (optExternalContent.isPresent()) {
            CornellContentEntity externalContent = optExternalContent.get();
            externalContent.setExternalId(externalId);
            externalContent.setCiosData(jsonNode);
            externalContent.setIsActive(isActive);
            externalContent.setCreatedDate(externalContent.getCreatedDate());
            externalContent.setUpdatedDate(currentTime);
            externalContent.setSourceData(externalContent.getSourceData());
            externalContent.setFileId(externalContent.getFileId());
            cornellContentRepository.save(externalContent);
            Map<String, Object> entityMap = objectMapper.convertValue(externalContent, Map.class);
            flattenContentData(entityMap);
            String uniqueId = partnerCode + "_" + externalContent.getExternalId();
            esUtilService.updateDocument(Constants.CIOS_CONTENT_INDEX_NAME, Constants.INDEX_TYPE,
                    uniqueId,
                    entityMap,
                    cbServerProperties.getElasticCiosContentJsonPath()
            );
            return externalContent;
        } else {
            throw new CiosContentException("Data not present in DB", HttpStatus.BAD_REQUEST);
        }

    }

    @Override
    public ResponseEntity<?> deleteNotPublishContent(DeleteContentRequestDto deleteContentRequestDto) {
        SBApiResponse response = SBApiResponse.createDefaultResponse(Constants.API_CB_PLAN_PUBLISH);
        String partnerCode=deleteContentRequestDto.getPartnerCode();
        List<String> externalIds = deleteContentRequestDto.getExternalId();
        List<CornellContentEntity> entities = cornellContentRepository.findByExternalIdInAndPartnerCode(externalIds,partnerCode);
        List<String> errors = new ArrayList<>();
        for (String id : externalIds) {
            Optional<CornellContentEntity> entityOpt = entities.stream()
                    .filter(entity -> entity.getExternalId().equals(id))
                    .findFirst();
            if (entityOpt.isPresent()) {
                CornellContentEntity entity = entityOpt.get();
                if (entity.getIsActive()) {
                    errors.add("External ID: " + id + " is live, cannot delete.");
                } else {
                    JsonNode ciosData = entity.getCiosData(); // Assuming this getter method exists
                    if (ciosData != null && ciosData.path("content").has("status")) {
                        String status = ciosData.path("content").get("status").asText();
                        if ("notInitiated".equalsIgnoreCase(status)) {
                            cornellContentRepository.delete(entity);
                            String uniqueId = deleteContentRequestDto.getPartnerCode() + "_" + entity.getExternalId();
                            esUtilService.deleteDocument(uniqueId, Constants.CIOS_CONTENT_INDEX_NAME);
                        } else {
                            errors.add("External ID: " + id + " cannot be deleted because its status is not 'notInitiated'.");
                        }
                    } else {
                        errors.add("External ID: " + id + " does not have a valid status in ciosData.");
                    }
                }
            } else {
                errors.add("External ID: " + id + " does not exist.");
            }
        }
        if (!errors.isEmpty()) {
            log.error("Validation errors: {}", errors);
            return buildErrorResponse(response, HttpStatus.BAD_REQUEST, String.join("\n", errors));
        }
        response.getResult().put(Constants.STATUS, Constants.SUCCESS);
        response.getResult().put(Constants.MESSAGE, "Content deleted successfully.");
        return ResponseEntity.ok(response);
    }

    private ResponseEntity<?> buildErrorResponse(SBApiResponse response, HttpStatus status, String errorMessage) {
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErr(errorMessage);
        response.setResponseCode(status);
        return ResponseEntity.status(status).body(response);
    }

}
