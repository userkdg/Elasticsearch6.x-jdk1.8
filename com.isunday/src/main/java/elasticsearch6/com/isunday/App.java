package elasticsearch6.com.isunday;

import static elasticsearch6.com.isunday.ES6Manager.getES6Client;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest.Item;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;

/**
 * Hello world!
 *
 */
public class App {
//	private static ES6Manager es6Manager = ES6Manager.getInstance();
	
	private static final Logger log = LoggerFactory.getLogger(App.class);
	
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws UnknownHostException, InterruptedException, ExecutionException {
		String hostname = "127.0.0.1";
		int port = 9300;//9200会与服务器端口冲突
		System.out.println("Hello World!");

		@SuppressWarnings("resource")
		TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
				.addTransportAddress(new TransportAddress(InetAddress.getByName(hostname), port));
//				.addTransportAddress(new TransportAddress(InetAddress.getByName("host2"), 9400));
		System.out.println(client.nodeName()+"<---nodeName  connectNodes--->"+client.connectedNodes());

		AdminClient admin = client.admin();
		ClusterAdminClient cluster = admin.cluster();
		Map<String, Object> jsonMap = new HashMap<String, Object>();
		jsonMap.put("user","kimchy");
		jsonMap.put("postDate",new Date());
		jsonMap.put("message","trying out Elasticsearch");


//		更新
//		UpdateRequest updateRequest = new UpdateRequest();
//		updateRequest.index("index");
//		updateRequest.type("type");
//		updateRequest.id("1");
//		updateRequest.doc();
//		client.update(updateRequest).get();
		
//		IndexResponse response = client.prepareIndex("twitter", "tweet","1")
//		        .setSource(JSON.toJSON(json), XContentType.JSON)
//		        .get();
		
		GetResponse response2 = client.prepareGet("twitter", "tweet", "1").execute().actionGet();
		GetResponse response3 = client.prepareGet("k", "type", "liLxvmABAoGMIVwak6Tx").setOperationThreaded(false).get();

	       //output
		String sourceAsString2 = response2.getSourceAsString();
		String sourceAsString3 = response3.getSourceAsString();
        System.out.println(sourceAsString2);
        System.out.println(sourceAsString3);
        System.out.println("response.version():"+response2.getVersion());
        
//        ctx._source.gender、ctx._source.user
        UpdateRequest updateRequest = new UpdateRequest("k", "type", "liLxvmABAoGMIVwak6Tx")
                .script(new Script("ctx._source.user=\"kdg11\""));
        
        ActionFuture<UpdateResponse> update = client.update(updateRequest);
        UpdateResponse updateResponse = update.get();
//        UpdateResponse updateResponse = client.prepareUpdate("k", "type", "liLxvmABAoGMIVwak6Tx")
//								        .setScript(new Script("ctx._source.user=\"kdg\""))
//								        .get();
        Result result = updateResponse.getResult();
        String name = result.name();
        System.out.println(name + "====>>>" + result.name().equalsIgnoreCase("updated"));
        
		// on shutdown
		client.close();

	}
	
	@Test
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
	
	@Test
	public void search(){
		SearchResponse response = getES6Client().prepareSearch("twitter", "k")
		        .setTypes("type1", "type2")
		        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
		        .setQuery(QueryBuilders.termQuery("multi", "kdg"))                 // Query
		     //   .setPostFilter(QueryBuilders.rangeQuery("age").from(12).to(18))     // Filter
		        .setFrom(0).setSize(60).setExplain(true)
		        .get();
		SearchHits hits = response.getHits();
		long totalHits = hits.totalHits;
		System.out.println("命中：" + totalHits);
		for (SearchHit hit : hits) {
			log.info(hit.getSourceAsString());
			System.out.println(hit.getSourceAsString());
		}
		
//		//read scroll documentation
//		do {
//		    for (SearchHit hit : response.getHits().getHits()) {
//		        //Handle the hit...
//		    	
//		    }
//		    response = getES6Client().prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
//		} while(response.getHits().getHits().length != 0); // Zero hits mark the end of the scroll and the while loop.

//		return null;
	}
	/**
	 * 批量API允许在单个请求中索引和删除多个文档
	 * @throws IOException 
	 */
	public static String bulk() throws IOException {
		BulkRequestBuilder bulkRequest = getES6Client().prepareBulk();

		// either use client#prepare, or use Requests# to directly build index/delete requests
		bulkRequest.add(getES6Client().prepareIndex("twitter", "tweet", "1")
		        .setSource(jsonBuilder()
		                    .startObject()
		                        .field("user", "kimchy")
		                        .field("postDate", new Date())
		                        .field("message", "trying out Elasticsearch")
		                    .endObject()
		                  )
		        );

		bulkRequest.add(getES6Client().prepareIndex("twitter", "tweet", "2")
		        .setSource(jsonBuilder()
		                    .startObject()
		                        .field("user", "kimchy")
		                        .field("postDate", new Date())
		                        .field("message", "another post")
		                    .endObject()
		                  )
		        );

		BulkResponse bulkResponse = bulkRequest.get();
		if (bulkResponse.hasFailures()) {
		    // process failures by iterating through each bulk response item
		}
		BulkItemResponse[] items = bulkResponse.getItems();
		for (BulkItemResponse b : items) {
			DocWriteResponse response = b.getResponse();
			System.out.println(response.getResult());
		}
		return null;
	}
	
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
		GetResponse getResponse = ES6Manager.getES6Client().get(getRequest).get();
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
		MultiGetRequestBuilder prepareMultiGet = ES6Manager.getES6Client().prepareMultiGet();
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
		  UpdateResponse updateResponse = ES6Manager.getES6Client()
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

		UpdateResponse response = ES6Manager.getES6Client().update(updateRequest).get();

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
	
	
}
