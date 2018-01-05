package elasticsearch6.com.isunday;

import static elasticsearch6.com.isunday.ES6Manager.getES6Client;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest.Item;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.mustache.SearchTemplateRequestBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;

/**
 * 通过学习 api for java{@link https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/index.html}
 * Elasticsearch 6.X && jdk1.8 (version 52)
 * 进行二次封装，相关类有@see {@link ES6Manager} {@link ES6Utils}
 * 具体业务乃需要更改
 * @author King
 *
 */
public class ES6Utils {
	private static Logger log = LoggerFactory.getLogger(ES6Utils.class);
//	private static final ES6Manager ES6_MANAGER = ES6Manager.getInstance(); 
	
	public static void main(String[] args) throws UnknownHostException {
		TransportClient es6Client = getES6Client();
		List<String> l = new ArrayList<String>();
		l.add("aaaaaaaaaa");
		l.add("bbbbbbbbb");
		IndexResponse indexResponse = es6Client.prepareIndex("kdg", "kdg_type","2").setSource(JSON.toJSON(l), XContentType.JSON).get();
		Result result = indexResponse.getResult();
		String lowercase = result.getLowercase(); 
		if(lowercase == "create")
			System.out.println(true);
		GetRequestBuilder getRequestBuilder = es6Client.prepareGet("k", "type", "liLxvmABAoGMIVwak6Tx");
		
		GetResponse getResponse = getRequestBuilder.get();
		String sourceAsString = getResponse.getSourceAsString();
		System.out.println(sourceAsString);
	}

//***********************************add**********************************//
	/**
	 * 创建索引
	 * @return
	 */
	public void createIndex() {
		IndexResponse indexResponse;
		try {
			indexResponse = getES6Client().prepareIndex("King DG", "King_type", "3").setSource(
					XContentFactory.jsonBuilder().startObject().field("kdg", true).field("age", "23").endObject())
					.get();
			String result = indexResponse.getResult().name();
			System.out.println("*********************index********:" + result);
		} catch (IOException e) {
			e.printStackTrace();
		}
//		return false;
	}
	/**
	 * 
	 * @param json
	 */
	public void createIndexByJson(String json){
		IndexResponse indexResponse = getES6Client().prepareIndex("twitter", "tweet")
		        .setSource(json, XContentType.JSON)
		        .get();
		String result = indexResponse.getResult().name();
		System.out.println("*********************index********:" + result);
	}
//***********************************search**********************************//
	/**
	 * 
	 * @param query
	 * @return
	 */
	public List<SearchHit> search(String query){
		SearchResponse response = getES6Client().prepareSearch("twitter", "k")
		        .setTypes("type1", "type2")
		        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
		        .setQuery(QueryBuilders.queryStringQuery("kdg"))              // Query
		     //   .setPostFilter(QueryBuilders.rangeQuery("age").from(12).to(18))     // Filter
		        .setFrom(0).setSize(60).setExplain(true)
		        .get();
		
		List<SearchHit> lSearchHits = new ArrayList<SearchHit>();
		
		SearchHits hits = response.getHits();
		long totalHits = hits.totalHits;
		System.out.println("命中：" + totalHits);
		for (SearchHit hit : hits) {
			log.info(hit.getSourceAsString() + hit.getScore());
			lSearchHits.add(hit);
			System.out.println(hit.getSourceAsString());
		}
		return lSearchHits;
	}
	
	/**
	 * 
	 */
	public void multiSearch() {
		SearchRequestBuilder srb1 = getES6Client().prepareSearch()
				.setQuery(QueryBuilders.queryStringQuery("elasticsearch"))
				.setSize(1);
		SearchRequestBuilder srb2 = getES6Client().prepareSearch()
				//.setQuery(QueryBuilders.matchQuery("name", "kdg"))
				.setSize(1);

		MultiSearchResponse sr = getES6Client().prepareMultiSearch().add(srb1).add(srb2).get();

		// You will get all individual responses from
		// MultiSearchResponse#getResponses()
		long nbHits = 0;

		org.elasticsearch.action.search.MultiSearchResponse.Item[] items = sr.getResponses();
		for (int i = 0; i < items.length; i++) {
			SearchResponse response = items[i].getResponse();
			nbHits += response.getHits().getTotalHits();
			SearchHits hits = response.getHits();
			for (SearchHit searchHit : hits) {
				Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
				Set<Entry<String, HighlightField>> entrySet = highlightFields.entrySet();
				for (Entry<String, HighlightField> entry : entrySet) {
					HighlightField highlightField = entry.getValue();
					Text[] fragments = highlightField.getFragments();
					for (Text text : fragments) {
						System.out.println("<span style=\"color:red;\">" + text.toString() + "</span>");
					}
				}
			}
		}
		log.info("nbHits:" + nbHits);
		System.out.println(nbHits);
	}
	
	public String searchByTemplate(){
		Map<String, Object> template_params = new HashMap<String, Object>();
		template_params.put("param_gender", "male");
		
		SearchResponse sr = new SearchTemplateRequestBuilder(getES6Client())
				.setScript("{\n" +                                  
			                "        \"query\" : {\n" +
			                "            \"match\" : {\n" +
			                "                \"gender\" : \"{{param_gender}}\"\n" +
			                "            }\n" +
			                "        }\n" +
			                "}")
//			    .setScript("template_gender")     
			    .setScriptType(ScriptType.INLINE) 
			    .setScriptParams(template_params)
			    
//			    .setScript("template_gender")                       
//		        .setScriptType(ScriptType.STORED)     
//		        .setScriptParams(template_params)                   
//		        .setRequest(new SearchRequest())       
		        
			    .setRequest(new SearchRequest())              
			    .get()                                        
			    .getResponse();
		SearchHits hits = sr.getHits();
		for (SearchHit searchHit : hits) {
			System.out.println(searchHit.getHighlightFields()+"....."+searchHit.getSourceAsString());
		}
		return null;
	}
	
//***********************************get**********************************//
	/**
	 * 
	 * @param index
	 * @param type
	 * @param id
	 * @return jsonStr
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	public static String get(String index, 
							 String type, 
							 String id ) throws InterruptedException, ExecutionException {
		GetRequest getRequest = new GetRequest(index, type, id);
		GetResponse getResponse = getES6Client().get(getRequest).get();
		String sourceAsString = getResponse.getSourceAsString();
		log.info("GetResponse:" + sourceAsString);
		return sourceAsString;
	}
	/**
	 * .add("twitter", "tweet", "1")           
	 * .add("twitter", "tweet", "2", "3", "4") 
	 * .add("another", "type", "foo")
	 * @param item
	 * @return
	 */
	public static String getMulti(List<Item> item){
		MultiGetRequestBuilder prepareMultiGet = getES6Client().prepareMultiGet();
		for (Item itm : item) {
			prepareMultiGet.add(itm);
		}
		List<Object> l = new ArrayList<Object>();
		MultiGetResponse multiGetItemResponses = prepareMultiGet.get();
		for (MultiGetItemResponse itemResponse : multiGetItemResponses) { 
		    GetResponse response = itemResponse.getResponse();
		    if (response.isExists()) {                      
		        String json = response.getSourceAsString();
		        l.add(json);
		    }
		}
		String jsonStr = JSON.toJSONString(l);
		log.info("JSON.toJSONString(l):" + JSON.toJSONString(l));
		return jsonStr;
	}
	//***********************************update**********************************//
	/**
	 * 通过script来更新数据
	 * Result 
	 * index type id 
	 * script ctx._source.user=\"kdg\"
	 * @return @see {@link Result} 
	 * @throws UnknownHostException
	 */
	public static String updateByScript(String index, 
										String type, 
										String id, 
										String script) {
		  UpdateResponse updateResponse = getES6Client()
	  									.prepareUpdate(index, type, id)
								        .setScript(new Script(script))
								        .get();

		  Result result = updateResponse.getResult(); 
		  System.out.println(result.name());
		  return result.name().toLowerCase();
	}
	 
	/**
	 * 通过UpdateRequest来更新数据
	 * @return {@link UpdateRequest}
	 * @throws Exception
	 */
	public static String updateByReq() throws Exception {
		UpdateRequest updateRequest = new UpdateRequest();
		updateRequest.index("twitter");
		updateRequest.type("tweet");
		updateRequest.id("1");
		updateRequest.doc(XContentFactory.jsonBuilder().startObject()
				// 对没有的字段添加, 对已有的字段替换
				.field("gender", "male1").field("message", "hello").endObject());

		UpdateResponse response = getES6Client().update(updateRequest).get();

		// 打印
		String index = response.getIndex();
		String type = response.getType();
		String id = response.getId();
		long version = response.getVersion();
		System.out.println(index + " : " + type + ": " + id + ": " + version);

		Result result = response.getResult();
		System.out.println(" 11 " + result.name().equals(Result.UPDATED));

		System.out.println(result.name() + "====>>>" + result.name().equalsIgnoreCase("updated"));
		return result.name();
    }
	
	//***********************************delete**********************************//
	public boolean delete(String index, String type, String id){
//		DeleteResponse deleteResponse = getES6Client().prepareDelete(index, type, id).execute().actionGet();
		DeleteResponse deleteResponse = getES6Client().prepareDelete(index, type, id).get();
		String status = deleteResponse.status().name();
		log.info("delete status....." + status);
		return true;
	}
	
	/**
	 * 
	 * @return
	 */
	public long deleteByQuery(){
		BulkByScrollResponse response =
			    DeleteByQueryAction.INSTANCE.newRequestBuilder(getES6Client())
			        .filter(QueryBuilders.matchQuery("gender", "male")) 
			        .source("persons")                                  
			        .get();                                             

		long deleted = response.getDeleted();
		return deleted;
	}
	
	/**
	 * asych的delete
	 */
	public void deleteByQuerySych(){
		DeleteByQueryAction.INSTANCE.newRequestBuilder(getES6Client())
		    .filter(QueryBuilders.matchQuery("gender", "male"))                  
		    .source("persons")                                                   
		    .execute(new ActionListener<BulkByScrollResponse>() {           
		        public void onResponse(BulkByScrollResponse response) {
		            long deleted = response.getDeleted();     
		            log.info("删除了" + deleted);
		        }
		        public void onFailure(Exception e) {
		            // Handle the exception
		        	log.info("失败" + e.getMessage());
		        }
		    });
	}
}
