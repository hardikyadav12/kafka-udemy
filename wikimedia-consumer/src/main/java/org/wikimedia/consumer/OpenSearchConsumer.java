package org.wikimedia.consumer;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Optional;

public class OpenSearchConsumer {

    public static RestHighLevelClient createRestHighLevelClient() {

        String connString = "http://localhost:9200";

        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);

        String userInfo = connUri.getUserInfo();

        if(Optional.ofNullable(userInfo).isEmpty()) {
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        }else{
            String[] auth = userInfo.split(":");
            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));

        }

        return restHighLevelClient;
    }



    public static void main(String[] args) throws IOException {

        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getName());
        RestHighLevelClient openSearchClient = createRestHighLevelClient();

        try(openSearchClient) {
            if(!openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT)) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            }else{
                log.info("The index already exists!");
            }
        }

    }
}
