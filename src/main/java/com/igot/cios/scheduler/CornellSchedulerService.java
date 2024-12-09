package com.igot.cios.scheduler;

import com.bazaarvoice.jolt.Chainr;
import com.bazaarvoice.jolt.JsonUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cios.dto.RequestBodyDTO;
import com.igot.cios.exception.CiosContentException;
import com.igot.cios.kafka.KafkaProducer;
import com.igot.cios.plugins.DataTransformUtility;
import com.igot.cios.util.CbServerProperties;
import com.igot.cios.util.Constants;
import com.igot.cios.util.PayloadValidation;
import com.igot.cios.util.transactional.cassandrautils.CassandraOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.web.client.RestTemplate;
import com.fasterxml.jackson.core.type.TypeReference;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

@Slf4j
@Service
public class CornellSchedulerService implements SchedulerInterface {

    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    KafkaProducer kafkaProducer;
    @Autowired
    PayloadValidation payloadValidation;
    @Autowired
    RestTemplate restTemplate;
    @Autowired
    private CbServerProperties cbServerProperties;
    @Autowired
    private CassandraOperation cassandraOperation;
    @Autowired
    private DataTransformUtility dataTransformUtility;

    public void callEnrollmentAPI(String partnerCode, JsonNode rawContentData) {
        try {
            log.info("CornellSchedulerService::callEnrollmentAPI");
            JsonNode entity = dataTransformUtility.fetchPartnerInfoUsingApi(partnerCode);
            List<Object> contentJson = objectMapper.convertValue(entity.get("transformProgressViaApi"), new TypeReference<List<Object>>() {});
            JsonNode transformData = dataTransformUtility.transformData(rawContentData, contentJson);
            String extCourseId = transformData.get("courseid").asText();
            String partnerId = entity.get("id").asText();
            JsonNode result = dataTransformUtility.callCiosReadApi(extCourseId,partnerId);
            String courseId = result.path("content").get("contentId").asText();
            String[] parts = transformData.get("userid").asText().split("@");
            ((ObjectNode) transformData).put("userid", parts[0]);
            String userId = transformData.get("userid").asText();
            log.info("courseId  and userid {} {}", courseId, userId);
            Map<String, Object> propertyMap = new HashMap<>();
            propertyMap.put("userid", userId);
            propertyMap.put("courseid", courseId);
            propertyMap.put("progress", 100);
            List<Map<String, Object>> listOfMasterData = cassandraOperation.getRecordsByProperties(Constants.KEYSPACE_SUNBIRD_COURSES, Constants.TABLE_USER_EXTERNAL_ENROLMENTS, propertyMap, null);
            if (CollectionUtils.isEmpty(listOfMasterData)) {
                Long date = Long.valueOf(transformData.get("completedon").asText());
                String formatedDate = updateDateFormatFromTimestamp(date);
                ((ObjectNode) transformData).put("completedon", formatedDate);
                ((ObjectNode) transformData).put("partnerCode", partnerCode);
                ((ObjectNode) transformData).put("partnerId", entity.get("id").asText());
                payloadValidation.validatePayload(Constants.PROGRESS_DATA_VALIDATION_FILE, transformData);
                kafkaProducer.push(cbServerProperties.getTopic(), transformData);
            } else {
                log.info("Progress updated 100 for user {}", userId);
            }
            log.info("callCornellEnrollmentAPI {} ", transformData.asText());
        } catch (Exception e) {
            log.error("error while processing", e);
            throw new CiosContentException(Constants.ERROR, e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private String updateDateFormatFromTimestamp(Long completedon) {
        Date date = new Date(completedon);
        SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
        return sdf.format(date);
    }

    public JsonNode loadEnrollment() {
        RequestBodyDTO requestBodyDTO = new RequestBodyDTO();
        requestBodyDTO.setServiceCode(cbServerProperties.getCornellEnrollmentServiceCode());
        requestBodyDTO.setUrlMap(formUrlMapForEnrollment());
        String payload = null;
        try {
            payload = objectMapper.writeValueAsString(requestBodyDTO);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return performEnrollmentCall(cbServerProperties.cornellPartnerCode,payload);
    }

    private Map<String, String> formUrlMapForEnrollment() {
        DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");
        LocalDate today = LocalDate.now();
        LocalDate startDate = today.minusDays(cbServerProperties.getCornellDateRange()); // Adjust the range as needed
        String completionRange = startDate.format(FORMATTER) + ":" + today.format(FORMATTER);
        log.info("Completion Range {}", completionRange);
        Map<String, String> urlMap = new HashMap<>();
        urlMap.put("offset", "0");
        urlMap.put("limit", cbServerProperties.getCornellEnrollmentListLimit());
        urlMap.put("course_type", cbServerProperties.getCornellEnrollmentListCourseType());
        urlMap.put("completion_range", completionRange);
        return urlMap;
    }

    public JsonNode performEnrollmentCall(String partnerCode, String requestBody) {
        log.info("calling service locator for getting {} enrollment list", partnerCode);
        String url = cbServerProperties.getServiceLocatorHost() + cbServerProperties.getServiceLocatorFixedUrl();
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Object> entity = new HttpEntity<>(requestBody, headers);
        ResponseEntity<Object> response = restTemplate.exchange(
                url,
                HttpMethod.POST,
                entity,
                Object.class
        );
        if (response.getStatusCode().is2xxSuccessful()) {
            JsonNode jsonNode = objectMapper.valueToTree(response.getBody());
            JsonNode jsonData = jsonNode.path("responseData").get("enrollments");
            jsonData.forEach(
                    eachContentData -> {
                        callEnrollmentAPI(partnerCode, eachContentData);
                    });
            return jsonData;
        } else {
            throw new RuntimeException("Failed to retrieve externalId. Status code: " + response.getStatusCodeValue());
        }
    }
}
