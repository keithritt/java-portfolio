package test;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import org.json.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.security.KeyStore;
import java.text.ParseException;
import javax.net.ssl.SSLSocketFactory;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpMessage;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.HttpVersion;
import org.apache.http.NameValuePair;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpParams;
import org.apache.http.params.HttpProtocolParams;
import org.apache.http.protocol.HTTP;
import org.apache.poi.util.SystemOutLogger;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class RestAPI {

	String ticket;
	String url_base = "https://bim360field.autodesk.com/";
	String projectId;

  public RestAPI() throws IOException, ParseException {
    ticket = logIn();
  }

  public void setProject(String projectName){
  	switch(projectName) {
  	case "sandbox":
  		projectId = "sandbox-xxxxxx";
  		break;
  	case "prod":
  		projectId = "prod-xxxxxx";
  		break;
  	}
  }

  private String logIn() throws IOException, ParseException {
   	final String username = "test@test.com";
    final String password = "secret";
    String url_suffix = "api/login";

    // set up api params
		List <NameValuePair> nvps = new ArrayList <NameValuePair>();
		nvps.add(new BasicNameValuePair("username", username));
		nvps.add(new BasicNameValuePair("password", password));

		String result = callApi(url_suffix, nvps);

		JSONObject myjson = new JSONObject(result);
		return myjson.get("ticket").toString();
	}

	private String callApi(String url_suffix, List <NameValuePair> nvps) throws ClientProtocolException, IOException {
		String url = url_base + url_suffix;
		String requestType = "";
		String ret = "";

		switch(url_suffix) {
			case "api/login":
			case "api/projects":
			case "api/get_equipment":
			case "api/get_categories":
			case "api/areas":
			case "api/custom_fields":
			case "api/vela_fields":
			case "api/get_checklists":
			case "api/get_checklist_headers":
			case "api/library/all_folders":
			case "api/library/all_files":
				requestType = "POST";
				break;
			case "fieldapi/checklists/v1":
				requestType = "GET";
				break;
			default:
				if(url_suffix.startsWith("fieldapi/checklists/v1"))
					requestType = "GET";
				else {
					Debug.pr("Unknown url_suffix:" + url_suffix);
					Debug.die();
				}
				break;
		}

		try {
			switch(requestType) {
			case "GET":
				url += "?";
				for (NameValuePair nvp : nvps) {
					url += nvp.getName()+"="+nvp.getValue()+"&";
				}

				URL urlObj = new URL(url);
		        URLConnection uc = urlObj.openConnection();
		        BufferedReader in = new BufferedReader(new InputStreamReader(uc.getInputStream()));
		        ret = in.readLine();

				break;

			case "POST":
				HttpPost request = new HttpPost(url);

			 	request.setHeader("Content-Type", "application/x-www-form-urlencoded");

				UrlEncodedFormEntity uf;
				uf = new UrlEncodedFormEntity(nvps, HTTP.UTF_8);
			 	request.setEntity(uf);

				HttpClient httpclient =  getNewHttpClient();
			 	HttpResponse response = httpclient.execute(request);
				HttpEntity entity = response.getEntity();
		        InputStream instream = entity.getContent();

				ret = convertStreamToString(instream);

				break;

			}
		}

		catch (FileNotFoundException e) {
			ret = "{}";
		}
		catch (Exception e) {
			Debug.pr(e.getMessage());
			e.printStackTrace();
			Debug.die();
		}

		return ret;
	}


	public JSONArray getEquipment(Integer limit, Integer offset) throws ClientProtocolException, ParseException, IOException {

		String url_suffix = "api/get_equipment";
		List <NameValuePair> nvps = new ArrayList <NameValuePair>();
		nvps.add(new BasicNameValuePair("ticket", ticket));
		nvps.add(new BasicNameValuePair("project_id", projectId));
		nvps.add(new BasicNameValuePair("limit", limit.toString()));
		nvps.add(new BasicNameValuePair("offset", offset.toString()));
		nvps.add(new BasicNameValuePair("details", "all"));

		return new JSONArray(callApi(url_suffix, nvps));
	}

	public JSONObject getChecklist(String checklistId) throws ClientProtocolException, ParseException, IOException {
		List <NameValuePair> nvps = new ArrayList <NameValuePair>();
		nvps = new ArrayList <NameValuePair>();
		nvps.add(new BasicNameValuePair("ticket", ticket));
		nvps.add(new BasicNameValuePair("project_id", projectId));

		String url_suffix = "fieldapi/checklists/v1/" + checklistId;
		String apiResult = callApi(url_suffix, nvps);

		return new JSONObject(apiResult);
	}

	public JSONArray getCustomFields() throws ClientProtocolException, IOException, ParseException {
		String url_suffix = "api/custom_fields";

        // set up api params
		List <NameValuePair> nvps = new ArrayList <NameValuePair>();
		nvps.add(new BasicNameValuePair("ticket", ticket));
		nvps.add(new BasicNameValuePair("project_id", projectId));

		String result = callApi(url_suffix, nvps);

		return new JSONArray(result);
	}

	public JSONObject getCategories() throws ClientProtocolException, IOException, ParseException {
    String url_suffix = "api/get_categories";

    // set up api params
		List <NameValuePair> nvps = new ArrayList <NameValuePair>();
		nvps.add(new BasicNameValuePair("ticket", ticket));
		nvps.add(new BasicNameValuePair("project_id", projectId));

		String result = callApi(url_suffix, nvps);

		return new JSONObject(result);
	}

	public JSONArray getAreas() throws ClientProtocolException, IOException, ParseException {
    Debug.pr("RestAPI->getAreas()");
		String url_suffix = "api/areas";

    // set up api params
		List <NameValuePair> nvps = new ArrayList <NameValuePair>();
		nvps.add(new BasicNameValuePair("ticket", ticket));
		nvps.add(new BasicNameValuePair("project_id", projectId));

		return new JSONArray(callApi(url_suffix, nvps));
	}

	public static HttpClient getNewHttpClient() {
	  try {
	    KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
	    trustStore.load(null, null);

	    SSLSocketFactoryEx sf = new SSLSocketFactoryEx(trustStore);
	    sf.setHostnameVerifier(SSLSocketFactoryEx.ALLOW_ALL_HOSTNAME_VERIFIER);

	    HttpParams params = new BasicHttpParams();
	    HttpProtocolParams.setVersion(params, HttpVersion.HTTP_1_1);
	    HttpProtocolParams.setContentCharset(params, HTTP.UTF_8);

	    SchemeRegistry registry = new SchemeRegistry();
	    registry.register(new Scheme("http", PlainSocketFactory.getSocketFactory(), 80));
	    registry.register(new Scheme("https", sf, 443));

	    ClientConnectionManager ccm = new ThreadSafeClientConnManager(params, registry);

	    return new DefaultHttpClient(ccm, params);
	  } catch (Exception e) {
	    return new DefaultHttpClient();
	  }
	}

	private static String convertStreamToString(InputStream is) {
	    BufferedReader reader = new BufferedReader(new InputStreamReader(is));
	    StringBuilder sb = new StringBuilder();

	    String line = null;
	    try {
	        while ((line = reader.readLine()) != null) {
	            sb.append(line + "\n");
	        }
	    } catch (IOException e) {
	        e.printStackTrace();
	    } finally {
	        try {
	            is.close();
	        } catch (IOException e) {
	            e.printStackTrace();
	        }
	    }
	    return sb.toString();
	}
}