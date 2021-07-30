package org.openpaas.servicebroker.cubrid.service.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class CUBRIDHttpsURLConnection {
	private Logger logger = LoggerFactory.getLogger(CUBRIDHttpsURLService.class);

	public static final String CM_IP = "192.168.2.205";
	public static final String CM_PORT = "8001";
	
	private static final String DEFAULT_CONNECTION_URL = "https://192.168.2.205:8001/cm_api";

	private String connectionUrl;

	private HttpsURLConnection connection;

	public void setConnection(HttpsURLConnection connection) {
		this.connection = connection;
	}

	public CUBRIDHttpsURLConnection() {
		super();
		this.connectionUrl = DEFAULT_CONNECTION_URL;
	}

	public CUBRIDHttpsURLConnection(String connectionUrl) {
		super();
		this.connectionUrl = connectionUrl;
	}

	public String getConnectionUrl() {
		return connectionUrl;
	}

	public void setConnectionUrl(String connectionUrl) {
		this.connectionUrl = connectionUrl;
	}

	public boolean openConnection() {
		if (connection != null) {
			return false;
		}

		TrustManager[] trustAllCerts = new TrustManager[] { new X509TrustManager() {
			public X509Certificate[] getAcceptedIssuers() {
				return null;
			}

			public void checkClientTrusted(X509Certificate[] certs, String authType) {
			}

			public void checkServerTrusted(X509Certificate[] certs, String authType) {
			}
		} };

		SSLContext sslContext = null;
		try {
			sslContext = SSLContext.getInstance("SSL");
			sslContext.init(null, trustAllCerts, new SecureRandom());
		} catch (KeyManagementException e) {
			logger.error(e.getMessage(), e);
		} catch (NoSuchAlgorithmException e) {
			logger.error(e.getMessage(), e);
		}

		HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory());

		HttpsURLConnection.setDefaultHostnameVerifier(new HostnameVerifier() {
			public boolean verify(String hostname, SSLSession session) {
				return true;
			}
		});

		URL url = null;
		try {
			url = new URL(connectionUrl);
		} catch (MalformedURLException e) {
			logger.error(e.getMessage(), e);
		}

		try {
			connection = (HttpsURLConnection) url.openConnection();
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		}

		// connection.setDefaultUseCaches(false);
		connection.setDoInput(true);
		connection.setDoOutput(true);

		try {
			connection.setRequestMethod("POST");
		} catch (ProtocolException e) {
			logger.error(e.getMessage(), e);
		}

		connection.setRequestProperty("Accept", "text/plain;charset=utf-8");
		connection.setRequestProperty("Content-type", "application/json");

		try {
			connection.connect();
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		}

		return true;
	}

	public void closeConnection() {
		if (connection != null) {
			connection.disconnect();
		}
	}

	public void sendRequest(JSONObject request) {
		try (OutputStream outputStream = connection.getOutputStream();
				OutputStreamWriter outputStreamWriter = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8);
				PrintWriter printWriter = new PrintWriter(outputStreamWriter)) {

			printWriter.write(request.toString());
			printWriter.flush();
		} catch (UnsupportedEncodingException e) {
			logger.error(e.getMessage(), e);
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		}
	}

	public JSONObject recvResponse() {
		if (connection == null) {
			return null;
		}

		StringBuilder response = new StringBuilder();

		try (InputStream inputStream = connection.getInputStream();
				InputStreamReader inputStreamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
				BufferedReader bufferedReader = new BufferedReader(inputStreamReader)) {

			String readLine = null;
			while ((readLine = bufferedReader.readLine()) != null) {
				response.append(readLine);
			}
		} catch (UnsupportedEncodingException e) {
			logger.error(e.getMessage(), e);
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		}

		return (new JSONObject(response.toString()));
	}
}
