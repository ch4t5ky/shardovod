package org.inviewclub.shardovod.opensearch;

import java.util.List;
import java.util.Map;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.util.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.rest_client.RestClientTransport;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class OpenSearch {
    private OpenSearchClient client;
    private RestClient restClient;
    private OpenSearchTransport transport;

    public boolean init(String host, int port, String protocol, String username, String password) {
        try {
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(username, password));

            restClient = RestClient.builder(new HttpHost(host, port, protocol))
                    .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                        @Override
                        public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                            return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                        }
                    })
                    .build();

            transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
            client = new OpenSearchClient(transport); // no cast needed [web:76]

            return true;
        } catch (Exception e) {
            e.printStackTrace();
            close(); // best-effort cleanup if partial init happened
            return false;
        }
    }

    public OpenSearchClient client() {
        if (client == null) throw new IllegalStateException("OpenSearch client not initialized");
        return client;
    }

    public void close() {
        try {
            if (restClient != null) {
                restClient.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            restClient = null;
            transport = null;
            client = null;
        }
    }

    public List<Map<String, Object>> getAllShards() {
    if (restClient == null) {
        throw new IllegalStateException("OpenSearch REST client not initialized");
    }

    try {
        Request request = new Request("GET", "/_cat/shards?format=json");
        Response response = restClient.performRequest(request);

        String json = EntityUtils.toString(response.getEntity());

        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(json, new TypeReference<List<Map<String, Object>>>() {});
    } catch (Exception e) {
        throw new RuntimeException("Failed to fetch shard info from /_cat/shards", e);
    }
}

}
