import com.refinitiv.ema.access.AckMsg;
import com.refinitiv.ema.access.ElementList;
import com.refinitiv.ema.access.EmaFactory;
import com.refinitiv.ema.access.GenericMsg;
import com.refinitiv.ema.access.Map;
import com.refinitiv.ema.access.MapEntry;
import com.refinitiv.ema.access.Msg;
import com.refinitiv.ema.access.OmmConsumer;
import com.refinitiv.ema.access.OmmConsumerClient;
import com.refinitiv.ema.access.OmmConsumerConfig;
import com.refinitiv.ema.access.OmmConsumerEvent;
import com.refinitiv.ema.access.RefreshMsg;
import com.refinitiv.ema.access.ReqMsg;
import com.refinitiv.ema.access.ServiceEndpointDiscovery;
import com.refinitiv.ema.access.ServiceEndpointDiscoveryClient;
import com.refinitiv.ema.access.ServiceEndpointDiscoveryEvent;
import com.refinitiv.ema.access.ServiceEndpointDiscoveryInfo;
import com.refinitiv.ema.access.ServiceEndpointDiscoveryOption;
import com.refinitiv.ema.access.ServiceEndpointDiscoveryResp;
import com.refinitiv.ema.access.StatusMsg;
import com.refinitiv.ema.access.UpdateMsg;

class AppClient implements ServiceEndpointDiscoveryClient, OmmConsumerClient {
	
	String host;
	String port;

	@Override
	public void onError(String errText, ServiceEndpointDiscoveryEvent arg1) {
		System.out.println(errText);
		
	}

	@Override
	public void onSuccess(ServiceEndpointDiscoveryResp serviceEndpointResp, ServiceEndpointDiscoveryEvent arg1) {
//		System.out.println(serviceEndpointResp);
		for(ServiceEndpointDiscoveryInfo info:serviceEndpointResp.serviceEndpointInfoList()) {
			// Print out only host and port
//			System.out.println(info.endpoint() + ":" + info.port());
			host = info.endpoint();
			port = info.port();
			break;
			
		}
		
	}

	@Override
	public void onAckMsg(AckMsg arg0, OmmConsumerEvent arg1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onAllMsg(Msg arg0, OmmConsumerEvent arg1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onGenericMsg(GenericMsg arg0, OmmConsumerEvent arg1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onRefreshMsg(RefreshMsg msg, OmmConsumerEvent arg1) {
		System.out.println(msg);
		
	}

	@Override
	public void onStatusMsg(StatusMsg msg, OmmConsumerEvent arg1) {
		System.out.println(msg);
		
	}

	@Override
	public void onUpdateMsg(UpdateMsg msg, OmmConsumerEvent arg1) {
		System.out.println(msg);
		
	}
	
}
public class ConsumerRto {
	public static void main (String[] args) {
		// Get Service Endpoint
		// Client ID, Client Secret (Replace with the valid credentials before running)

		String clientId = "GE-12345678910";
    String clientSecret = "1dsf9089-43rt-t43t-5yre-143fhgf1sert";
    
		// Prepare appClient
		AppClient appClient = new AppClient();
		
		// Set options
		ServiceEndpointDiscoveryOption options = EmaFactory.createServiceEndpointDiscoveryOption();
		options.clientId(clientId);
		options.clientSecret(clientSecret);
		options.transport(ServiceEndpointDiscoveryOption.TransportProtocol.TCP);
		
		// Use ServiceEndpoint from EMA
		ServiceEndpointDiscovery service = EmaFactory.createServiceEndpointDiscovery();
		service.registerClient(options, appClient);
		
		System.out.println("Host: " + appClient.host);
		System.out.println("Port: " + appClient.port);
		
		// Connect to Real-Time Optimized using programmatic configuration
		Map configDb = EmaFactory.createMap();	

		Map elementMap = EmaFactory.createMap();
		ElementList elementList = EmaFactory.createElementList();
		ElementList innerElementList = EmaFactory.createElementList();

		innerElementList.add(EmaFactory.createElementEntry().ascii("Channel", "Channel_1"));

		elementMap.add(EmaFactory.createMapEntry().keyAscii("Consumer_1", MapEntry.MapAction.ADD, innerElementList));
		innerElementList.clear();

		elementList.add(EmaFactory.createElementEntry().map("ConsumerList", elementMap));
		elementMap.clear();

		configDb.add(EmaFactory.createMapEntry().keyAscii("ConsumerGroup", MapEntry.MapAction.ADD, elementList));
		elementList.clear();

		innerElementList.add(EmaFactory.createElementEntry().ascii("ChannelType", "ChannelType::RSSL_ENCRYPTED"));

		innerElementList.add(EmaFactory.createElementEntry().ascii("Host", appClient.host));
		innerElementList.add(EmaFactory.createElementEntry().ascii("Port", appClient.port));
		innerElementList.add(EmaFactory.createElementEntry().intValue("EnableSessionManagement", 1));

		elementMap.add(EmaFactory.createMapEntry().keyAscii("Channel_1", MapEntry.MapAction.ADD, innerElementList));
		innerElementList.clear();

		elementList.add(EmaFactory.createElementEntry().map("ChannelList", elementMap));
		elementMap.clear();

		configDb.add(EmaFactory.createMapEntry().keyAscii("ChannelGroup", MapEntry.MapAction.ADD, elementList));
		elementList.clear();
		// End of the Real-Time Optimized using programmatic configuration
		
		OmmConsumerConfig config = EmaFactory.createOmmConsumerConfig();
		config.config(configDb);
		
		String tokenServiceUrlV2 = "https://api.refinitiv.com/auth/oauth2/v2/token";
		
		config.consumerName("Consumer_1");
		config.clientId(clientId);
		config.clientSecret(clientSecret);
		config.tokenServiceUrlV2(tokenServiceUrlV2);
		
		OmmConsumer consumer = EmaFactory.createOmmConsumer(config);
		// Data Subscription
		ReqMsg req = EmaFactory.createReqMsg();
		req.name("EUR=");
		req.serviceName("ELEKTRON_DD");
		
		consumer.registerClient(req, appClient);
		
		try {
			Thread.sleep(1000000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
