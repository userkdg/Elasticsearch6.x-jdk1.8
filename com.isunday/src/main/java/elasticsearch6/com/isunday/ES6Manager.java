package elasticsearch6.com.isunday;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;


public class ES6Manager {
	private static ES6Manager singleton;
	private static boolean isCluster;// false
	private static TransportClient c;
	private static Settings settings;
	private static final String host_1 = "127.0.0.1";
	private static final int port_1 = 9300;//9200会冲突

	private ES6Manager() {
	}
	
	public static ES6Manager getInstance(){
		 if (null == singleton) {  
	            synchronized (ES6Manager.class) {  
	                if (null == singleton) {  
	                    singleton = new ES6Manager();  
	                }  
	            }  
	        }  
		return singleton;
	}
	
	@SuppressWarnings("unchecked")
	public static TransportClient getES6Client() {
		try {
			if (null == settings) {
				settings = Settings.EMPTY;
			}
			if (isCluster) {
				if(null == c) {
					c = new PreBuiltTransportClient(settings)
							.addTransportAddress(new TransportAddress(InetAddress.getByName(host_1), port_1))
							.addTransportAddress(new TransportAddress(InetAddress.getByName(host_1), port_1));
				}
			}
			if (null == c) {
				c = new PreBuiltTransportClient(settings)
						.addTransportAddress(new TransportAddress(InetAddress.getByName(host_1), port_1));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return c;
	}
	
	public static void closeClient(){
		if(c != null) {
			c.close();
		}
	}
	
}
