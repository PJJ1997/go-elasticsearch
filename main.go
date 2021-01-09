package elasticsearch

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/pkg/errors"
)

// es查询返回结构体构造
type ESDocument struct {
	OuterHits struct {
		Total struct {
			Value int32 `json:"value"`
		} `json:"total"`
		InnerHits []struct {
			Score  float32 `json:"_score"`
			ID     string  `json:"_id"`
			Source Source  `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}

// 真正存数据的地方
type Source struct {
	EntityID   string `json:"entity_id"`
	EntityType int    `json:"entity_type"`
	// 假设文档中存有对象数组
	RelationEntities []Entity `json:"related_entities"`
}

type Entity struct {
	EntityID   string `json:"entity_id"`
	EntityType int    `json:"entity_type"`
}

func main() {
	client, _ := connectToElasticsearch()
	query := sizeFromQuery()
	response, _ := performESQuery(client, "indexName", query)
	results := ESDocument{}
	json.Unmarshal([]byte(response), &results)
	for _, v := range results.OuterHits.InnerHits {
		fmt.Println(v.Source.EntityID)
		fmt.Println(v.Source.EntityType)
		fmt.Println(v.Source.RelationEntities)
	}

	// 滚动查询用法（一次过查询大数据量）
	// es的size最多只能支持10000条
	// esDocuments := []*ESDocument{}
	esDocuments := make([]*ESDocument, 0)
	var scrollID string
	// 查询数据量：5 * 50000
	for i := 0; i <= 5 && (i == 0 || scrollID != ""); i += 5000 {
		from := int(i)
		size := 5000
		esPaginateResponse, tempScollID, err := scrollSearch(from, size, scrollID, "IndexName", client)
		if err != nil {
			return
		}
		scrollID = tempScollID
		esDocuments = append(esDocuments, esPaginateResponse)
	}
	fmt.Println("esDocuments: ", esDocuments)
}

// 创建 ESClient
func connectToElasticsearch() (*elasticsearch.Client, error) {
	// Save config as global variable
	var cfg = elasticsearch.Config{
		Addresses: []string{
			"127.0.0.1",
		},
		Username: "root",
		Password: "123456",
		Transport: &http.Transport{
			MaxIdleConnsPerHost:   10,
			ResponseHeaderTimeout: time.Duration(30) * time.Second,
			DialContext:           (&net.Dialer{Timeout: time.Second}).DialContext,
			TLSClientConfig: &tls.Config{
				MinVersion:         tls.VersionTLS11,
				InsecureSkipVerify: true,
			},
		},
	}
	return elasticsearch.NewClient((cfg))
}

//  执行 ES query 查询，返回字符串
func performESQuery(ESClient *elasticsearch.Client, index string, query map[string]interface{}) (string, error) {
	var buf bytes.Buffer

	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return "", errors.WithStack(err)
	}
	res, err := ESClient.Search(
		ESClient.Search.WithContext(context.Background()),
		ESClient.Search.WithIndex(index),
		ESClient.Search.WithBody(&buf),
		ESClient.Search.WithTrackTotalHits((true)),
		ESClient.Search.WithPretty(),
	)
	if err != nil {
		return "", errors.WithStack(err)
	}
	defer res.Body.Close()

	if res.IsError() {
		var e map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			return "", fmt.Errorf("Error parsing the response body: %s", err)
		}
		return "", fmt.Errorf("[%s] %s: %s",
			res.Status(),
			e["error"].(map[string]interface{})["type"],
			e["error"].(map[string]interface{})["reason"])
	}
	// 将字节流转换成字符流
	var sb strings.Builder
	buffer := make([]byte, 256)
	for {
		n, err := res.Body.Read(buffer)
		sb.Write(buffer[:n])
		if err != nil {
			if err != io.EOF {
				log.Println("read error:", err)
			}
			break
		}
	}
	return sb.String(), nil
}

// 分页 query
func sizeFromQuery() map[string]interface{} {
	query := map[string]interface{}{
		"size": 10,
		"from": 20,
	}
	return query
}

// 指定字段排序 query
func sortQuery() map[string]interface{} {
	query := map[string]interface{}{
		"sort": []map[string]interface{}{
			map[string]interface{}{
				"publish_time": map[string]interface{}{
					"order": "desc",
				},
			},
		},
	}
	return query
}

// 指定范围 query
func rangeQuery() map[string]interface{} {
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"range": map[string]interface{}{
				"publish_time": []map[string]interface{}{
					{
						"gte": "2020-01-02 00:00:00",
					},
					{
						"lte": "2020-01-03 00:00:00",
					},
				},
			},
		},
	}
	return query
}

// and 条件连接 query
func mustQuery() map[string]interface{} {
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []map[string]interface{}{
					{
						"match": map[string]interface{}{
							"entity_id": "123",
						},
					},
					{
						"match": map[string]interface{}{
							"entity_type": "456",
						},
					},
				},
			},
		},
	}
	return query
}

// or 条件连接 query
func shouldQuery() map[string]interface{} {
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"should": []map[string]interface{}{
					{
						"term": map[string]interface{}{
							"entity_id": "123",
						},
					},
					{
						"term": map[string]interface{}{
							"entity_type": "456",
						},
					},
				},
			},
		},
	}
	return query
}

// 如果文档中存在对象，根据指定对象的字段查找 query
func nestedQuery() map[string]interface{} {
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": map[string]interface{}{
					"nested": map[string]interface{}{
						"path": "related_entities",
						"query": map[string]interface{}{
							"bool": map[string]interface{}{
								"must": []map[string]interface{}{
									{
										"match": map[string]interface{}{
											"related_entities.entity_id": "123",
										},
									},
									{
										"match": map[string]interface{}{
											"related_entities.entity_type": "456",
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
	return query
}

// 保证至少满足n个should条件 query
func minimumShouldMatchQuery() map[string]interface{} {
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"should": []map[string]interface{}{
					{}, {},
				},
				"minimum_should_match": 1,
			},
		},
	}
	return query
}

// 一般用于类型为text的字段 会分词 分词后只要这个字符串命中一部分就会返回 query
func matchQuery() map[string]interface{} {
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []map[string]interface{}{
					{
						"match": map[string]interface{}{
							"entity_id": "123",
						},
					},
				},
			},
		},
	}
	return query
}

// 会分词 分词后这个字符串必须命中所有的词才会返回 query
func matchPhraseQuery() map[string]interface{} {
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"should": []map[string]interface{}{
					{
						"match_phrase": map[string]interface{}{
							"entity_id": map[string]interface{}{
								"query": "123",
							},
						},
					},
				},
			},
		},
	}
	return query
}

// https://my.oschina.net/u/3777515/blog/4700962
// 调节各个查询条件的文档的得分 要与function_score连用 query
func boostQuery() map[string]interface{} {
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"should": []map[string]interface{}{
					{
						"match_phrase": map[string]interface{}{
							"entity_id": map[string]interface{}{
								"query": "123",
								"boost": "3",
							},
						},
					},
					{
						"match_phrase": map[string]interface{}{
							"entity_type": map[string]interface{}{
								"query": "123",
								"boost": "1",
							},
						},
					},
				},
			},
		},
	}
	return query
}

// 计算特定条件下的文档的function_score
func scriptScoreQuery() map[string]interface{} {
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"function_score": map[string]interface{}{
				"query": map[string]interface{}{
					"bool": map[string]interface{}{
						"must": []map[string]interface{}{},
					},
				},
				"script_score": map[string]interface{}{
					"script": map[string]interface{}{
						"source": "doc[rank_score].value*0.01", //rank_score是文档的一个自定义的字段，想用什么字段来调分数都行
					},
				},
				"boost_mode": "replace", //sum
			},
		},
	}
	return query
}

// 第一次滚动查询时需要要调用，返回scollID，供下一次滚动查询调用
func PerformESQueryAndBuildScroll(query map[string]interface{}, index string, esClient *elasticsearch.Client) ([]map[string]interface{}, string, error) {

	startTime := time.Now()

	resultList := make([]map[string]interface{}, 0)
	var err error

	var reqBody bytes.Buffer
	err = json.NewEncoder(&reqBody).Encode(query)
	if err != nil {
		err = fmt.Errorf("encode query failed, %v", err)
		return resultList, "", errors.WithStack(err)
	}
	res, err := esClient.Search(
		esClient.Search.WithContext(context.Background()),
		esClient.Search.WithIndex(string(index)),
		esClient.Search.WithBody(&reqBody),
		esClient.Search.WithTrackTotalHits(true),
		esClient.Search.WithPretty(),
		esClient.Search.WithTimeout(5*60*time.Second),
		esClient.Search.WithScroll(time.Minute),
	)
	if err != nil {
		err = fmt.Errorf("Error getting response: %s", err)
		return resultList, "", errors.WithStack(err)
	}
	defer res.Body.Close()

	if res.IsError() {
		var e map[string]interface{}
		if err = json.NewDecoder(res.Body).Decode(&e); err != nil {
			err = fmt.Errorf("Error parsing the response body: %s", err)
		} else {
			err = fmt.Errorf("[%s] %s: %s", res.Status(),
				e["error"].(map[string]interface{})["type"],
				e["error"].(map[string]interface{})["reason"])
		}
		return resultList, "", errors.WithStack(err)
	}

	result := make(map[string]interface{})
	if err = json.NewDecoder(res.Body).Decode(&result); err != nil {
		err = fmt.Errorf("Error parsing the response body: %s", err)
		return resultList, "", errors.WithStack(err)
	}
	resultList = append(resultList, result)

	hits := result["hits"].(map[string]interface{})["hits"].([]interface{})

	scrollID := ""
	if len(hits) == query["size"].(int) {
		scrollID = result["_scroll_id"].(string)
	}

	// test code
	if len(hits) > 0 {
		log.Println("")
		log.Println("---------------- First level of PerformESQueryAndBuildScroll ---------------")
		length := len(hits)
		if length > 1 {
			length = 1
		}
		resultJSON, _ := json.Marshal(hits[0:length])
		log.Printf("adam Build scroll, result[%+v]", string(resultJSON))
		log.Printf("adam Build scroll, len(hits): [%+v], query time[%+v]", len(hits), time.Now().Sub(startTime))
	}

	return resultList, scrollID, err
}

// 调用第一次滚动查询方法，将返回结果封装好
func GetESDataAndBuildScroll(query map[string]interface{}, index string, esClient *elasticsearch.Client) (*ESDocument, string, error) {
	resultList, scrollID, err := PerformESQueryAndBuildScroll(query, index, esClient)
	if err != nil {
		return nil, "", errors.WithStack(err)
	}

	response := ESDocument{}
	for _, v := range resultList {
		res := new(ESDocument)
		jsonData, err := json.Marshal(v)
		if err != nil {
			return nil, "", errors.WithStack(err)
		}
		err = json.Unmarshal(jsonData, &res)
		if err != nil {
			return nil, "", errors.WithStack(err)
		}

		response.OuterHits.Total.Value = res.OuterHits.Total.Value
		response.OuterHits.InnerHits = append(response.OuterHits.InnerHits, res.OuterHits.InnerHits...)
	}

	return &response, scrollID, nil
}

// 第二次及以上调用滚动查询，根据scrollID查询
func PerformESQueryWithScroll(scrollID string, esClient *elasticsearch.Client) ([]map[string]interface{}, string, error) {
	if scrollID == "" {
		return nil, "", fmt.Errorf("=========================*************scrollID can not be empty in adam.PerformESQueryWithScroll")
	}

	resultList := make([]map[string]interface{}, 0)
	startTime := time.Now()

	res, err := esClient.Scroll(
		esClient.Scroll.WithContext(context.Background()),
		esClient.Scroll.WithPretty(),
		esClient.Scroll.WithScrollID(scrollID),
		esClient.Scroll.WithScroll(time.Minute),
	)
	if err != nil {
		err = fmt.Errorf("Error getting response: %s", err)
		return resultList, "", errors.WithStack(err)
	}
	defer res.Body.Close()

	if res.IsError() {
		var e map[string]interface{}
		if err = json.NewDecoder(res.Body).Decode(&e); err != nil {
			err = fmt.Errorf("Error parsing the response body: %s", err)
		} else {
			err = fmt.Errorf("[%s] %s: %s", res.Status(),
				e["error"].(map[string]interface{})["type"],
				e["error"].(map[string]interface{})["reason"])
		}
		return resultList, "", errors.WithStack(err)
	}

	result := make(map[string]interface{})
	if err = json.NewDecoder(res.Body).Decode(&result); err != nil {
		err = fmt.Errorf("Error parsing the response body: %s", err)
		return resultList, "", errors.WithStack(err)
	}

	resultList = append(resultList, result)

	scrollID = ""
	hits := result["hits"].(map[string]interface{})["hits"].([]interface{})
	if len(hits) > 0 {
		scrollID = result["_scroll_id"].(string)
	}

	// test code
	if len(hits) > 0 {
		log.Println("---------------- Second level of PerformESQueryWithScroll ---------------")
		resultJSON, _ := json.Marshal(hits[0])
		log.Printf("adam PerformESQueryWithScroll， len(resultList)[%d], len(hits)[%d], query time: [%+v]",
			len(resultList), len(hits), time.Now().Sub(startTime))
		log.Printf("adam PerformESQueryWithScroll， hits[0]: [%+v]", string(resultJSON))
	}

	return resultList, scrollID, nil
}

// 调用第二次及以上的滚动查询方法，将返回结果封装好
func GetESDataWithScroll(scrollID string, esClient *elasticsearch.Client) (*ESDocument, string, error) {
	resultList, scrollID, err := PerformESQueryWithScroll(scrollID, esClient)
	if err != nil {
		return nil, "", errors.WithStack(err)
	}

	response := ESDocument{}
	for _, v := range resultList {
		res := new(ESDocument)
		jsonData, err := json.Marshal(v)
		if err != nil {
			return nil, "", errors.WithStack(err)
		}
		err = json.Unmarshal(jsonData, &res)
		if err != nil {
			return nil, "", errors.WithStack(err)
		}

		response.OuterHits.Total.Value = res.OuterHits.Total.Value
		response.OuterHits.InnerHits = append(response.OuterHits.InnerHits, res.OuterHits.InnerHits...)
	}

	return &response, scrollID, nil
}

func scrollSearch(from, size int, scrollID string, esIndex string, client *elasticsearch.Client) (*ESDocument, string, error) {
	queryResponse := new(ESDocument)
	indexName := esIndex

	query := map[string]interface{}{
		"size": size,
		// 其他查询条件...
	}
	// 第一页查询，保证最后一页之后 scrollID 为空时不再执行查询
	if from == 0 {
		queryResponse, scrollIDResult, err := GetESDataAndBuildScroll(query, indexName, client)
		if err != nil {
			return nil, "", err
		}

		return queryResponse, scrollIDResult, nil
	}
	if scrollID != "" {
		queryResponse, scrollIDResult, err := GetESDataWithScroll(scrollID, client)
		if err != nil {
			return nil, scrollIDResult, err
		}

		return queryResponse, scrollIDResult, nil
	}

	return queryResponse, "", nil
}

// ================================ es 的删除更新插入 ================================

// 批量插入数据
func performESInsert(client elasticsearch.Client, index string, documents []interface{}) error {
	if len(documents) == 0 {
		return nil
	}
	requestBody, err := getInsertRequestBody(index, documents)
	if err != nil {
		log.Fatalf("Error getting request body: %s", err)
		return err
	}
	return performESBulk(client, index, requestBody)
}

func getInsertRequestBody(index string, documents []interface{}) (string, error) {
	var bodyBuf bytes.Buffer
	for _, document := range documents {
		documentValue := reflect.ValueOf(document).Elem()
		createHeader :=
			map[string]interface{}{
				"create": map[string]interface{}{
					"_index": index,
					"_id":    documentValue.FieldByName("ID").String(),
					"_type":  "_doc",
				},
			}
		header, err := json.Marshal(createHeader)
		if err != nil {
			return "", err
		}
		bodyBuf.Write(header)
		bodyBuf.WriteByte('\n')
		content, err := json.Marshal(document)
		if err != nil {
			// TODO: LOG
			return "", err
		}
		bodyBuf.Write(content)
		bodyBuf.WriteByte('\n')
	}
	return bodyBuf.String(), nil
}

// 批量更新插入数据，有就更新，没有就插入
func performESUpsert(client elasticsearch.Client, index string, documents []interface{}) error {
	requestBody, err := getUpsertRequestBody(index, documents)
	if err != nil {
		log.Fatalf("Error getting request body: %s", err)
		return err
	}
	return performESBulk(client, index, requestBody)
}

func getUpsertRequestBody(index string, documents []interface{}) (string, error) {
	var bodyBuf bytes.Buffer
	for _, document := range documents {
		documentValue := reflect.ValueOf(document).Elem()
		upsertHeader :=
			map[string]interface{}{
				"update": map[string]interface{}{
					"_index": index,
					"_id":    documentValue.FieldByName("ID").String(),
					"_type":  "_doc",
					// 失败重试 3 次
					"retry_on_conflict": 3,
				},
			}
		header, err := json.Marshal(upsertHeader)
		if err != nil {
			// TODO: LOG
			return "", err
		}
		bodyBuf.Write(header)
		bodyBuf.WriteByte('\n')

		upsertBody :=
			map[string]interface{}{
				"doc":           document,
				"doc_as_upsert": true,
			}
		content, err := json.Marshal(upsertBody)
		if err != nil {
			// TODO: LOG
			return "", err
		}
		bodyBuf.Write(content)
		bodyBuf.WriteByte('\n')
	}
	return bodyBuf.String(), nil
}

// 删除整个索引
func deleteESIndex(client elasticsearch.Client, index string) error {
	indexes := []string{index}
	req := esapi.IndicesDeleteRequest{
		Index: indexes,
	}
	// Perform the request with the client.
	res, err := req.Do(context.Background(), &client)
	if err != nil {
		log.Fatalf("delete ES all documents, error getting response: %s", err)
		return err
	}
	defer res.Body.Close()
	if res.IsError() {
		log.Printf("delete ES all documents, [%s] error indexing document", res.Status())
		return err
	}
	return nil
}

// 批量删除索引数据
func performESDelete(client elasticsearch.Client, index string, ids []string) error {
	for i := 0; i < len(ids); i += 20000 {
		endIndex := i + 20000
		if endIndex > len(ids) {
			endIndex = len(ids)
		}
		var bodyBuf bytes.Buffer
		for _, id := range ids[i:endIndex] {
			deleteHeader :=
				map[string]interface{}{
					"delete": map[string]interface{}{
						"_index": index,
						"_id":    id,
					},
				}
			header, err := json.Marshal(deleteHeader)
			if err != nil {
				return err
			}
			bodyBuf.Write(header)
			bodyBuf.WriteByte('\n')
		}
		err := performESBulk(client, index, bodyBuf.String())
		if err != nil {
			return err
		}
	}
	return nil
}

// 创建索引
func createZeusESIndex(client elasticsearch.Client) {
	body := map[string]interface{}{
		"aliases": map[string]interface{}{},
		"mappings": map[string]interface{}{
			"dynamic_templates": []interface{}{
				map[string]interface{}{
					"ik_fields": map[string]interface{}{
						"path_match":         "ik.*",
						"match_mapping_type": "string",
						"mapping": map[string]interface{}{
							"analyzer":        "ik_max_word",
							"search_analyzer": "ik_smart",
							"type":            "text",
						},
					},
				},
				map[string]interface{}{
					"whitespace_fields": map[string]interface{}{
						"path_match":         "ws.*",
						"match_mapping_type": "string",
						"mapping": map[string]interface{}{
							"analyzer": "whitespace",
							"type":     "text",
						},
					},
				},
				map[string]interface{}{
					"standard_fields": map[string]interface{}{
						"path_match":         "sd.*",
						"match_mapping_type": "string",
						"mapping": map[string]interface{}{
							"analyzer": "standard",
							"type":     "text",
						},
					},
				},
				map[string]interface{}{
					"keyword_fields": map[string]interface{}{
						"path_match":         "kw.*",
						"match_mapping_type": "string",
						"mapping": map[string]interface{}{
							"analyzer": "standard",
							"type":     "keyword",
						},
					},
				},
				map[string]interface{}{
					"not_indexed_fields": map[string]interface{}{
						"path_match":         "ni.*",
						"match_mapping_type": "string",
						"mapping": map[string]interface{}{
							"enabled": false,
							"type":    "object",
						},
					},
				},
			},
			// 因为signals是对象，所以套多了一层properties
			"properties": map[string]interface{}{
				"signals": map[string]interface{}{
					"properties": map[string]interface{}{
						"score": map[string]interface{}{
							"type": "float",
						},
						"signal_id": map[string]interface{}{
							"type": "keyword",
						},
					},
				},
			},
		},
	}
	jsonBody, _ := json.Marshal(body)
	fmt.Println(string(jsonBody))
	req := esapi.IndicesCreateRequest{
		Index: "indexName",
		Body:  bytes.NewReader(jsonBody),
	}
	res, err := req.Do(context.Background(), &client)
	if err != nil {
		return
	}
	defer res.Body.Close()
	fmt.Println(res.String())
}

// 批量操作数据公用方法
func performESBulk(client elasticsearch.Client, index string, requestBody string) error {
	// Set up the request object.
	req := esapi.BulkRequest{
		Index:   index,
		Body:    strings.NewReader(requestBody),
		Refresh: "false",
		Pretty:  false,
	}

	// Perform the request with the client.
	res, err := req.Do(context.Background(), &client)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.IsError() {
		return err
	}
	// Deserialize the response into a map.
	var r map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
	}
	return nil
}
