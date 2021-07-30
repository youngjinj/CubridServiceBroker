package org.openpaas.servicebroker.cubrid.service.impl;

import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Iterator;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.google.common.hash.Hashing;

@Service
public class CUBRIDHttpsURLService {
	private Logger logger = LoggerFactory.getLogger(CUBRIDHttpsURLService.class);

	private static final String CM_ADMIN_ID = "admin";
	private static final String CM_ADMIN_PASSWORD = "password";
	
	public static final String DEFAULT_CLIENT_VERSION = "11.0";

	private String token = null;

	public CUBRIDHttpsURLService() {
		super();
	}

	private String getRandomPassword() {
		final int length = 16;

		String AlphabetNumberSpecialCharacter = "ABCDEFGHIJKLMNOPQRSTUVWXYZ" + "abcdefghijklmnopqrstuvwxyz"
				+ "0123456789";

		StringBuilder password = new StringBuilder();

		SecureRandom secureRandom = null;
		try {
			secureRandom = SecureRandom.getInstance("SHA1PRNG");
			secureRandom.setSeed(secureRandom.generateSeed(length));
		} catch (NoSuchAlgorithmException e) {
			logger.error(e.getMessage(), e);
		}

		for (int i = 0; i < length; i++) {
			int index = secureRandom.nextInt(AlphabetNumberSpecialCharacter.length());
			password.append(AlphabetNumberSpecialCharacter.charAt(index));
		}

		return password.toString();
	}

	public String checkStatus(JSONObject response) {
		if (response != null && response.get("status") != null) {
			return response.get("status").toString();
		}

		return null;
	}
	
	public String getDbname(String dbname) {
		return Hashing.sha256().hashString(dbname, StandardCharsets.UTF_8).toString().substring(0, 15);
	}

	public JSONObject doLogin() {
		return doLogin(CM_ADMIN_ID, CM_ADMIN_PASSWORD, DEFAULT_CLIENT_VERSION);
	}

	public JSONObject doLogin(String id, String password) {
		return doLogin(id, password, DEFAULT_CLIENT_VERSION);
	}

	public JSONObject doLogin(String id, String password, String clientver) {
		if (token != null) {
			logger.warn("Already login.");

			return new JSONObject().put("status", "success");
		}

		CUBRIDHttpsURLConnection cubridHttpsURLConnection = new CUBRIDHttpsURLConnection();
		cubridHttpsURLConnection.openConnection();

		String task = "login";

		JSONObject request = new JSONObject().put("task", task).put("id", id).put("password", password).put("clientver",
				clientver);
		logger.debug(request.toString(4));

		cubridHttpsURLConnection.sendRequest(request);

		JSONObject response = cubridHttpsURLConnection.recvResponse();
		logger.debug(response.toString(4));

		cubridHttpsURLConnection.closeConnection();

		if (response != null && response.get("token") != null) {
			this.token = response.get("token").toString();
		}

		return response;
	}

	public JSONObject doLogout() {
		if (this.token == null) {
			JSONObject loginResponse = doLogin();

			if ("success".equals(checkStatus(loginResponse)) == false) {
				return null;
			}
		}

		CUBRIDHttpsURLConnection cubridHttpsURLConnection = new CUBRIDHttpsURLConnection();
		cubridHttpsURLConnection.openConnection();

		String token = this.token;
		String task = "logout";

		JSONObject request = new JSONObject().put("task", task).put("token", token);
		logger.debug(request.toString(4));

		cubridHttpsURLConnection.sendRequest(request);

		JSONObject response = cubridHttpsURLConnection.recvResponse();
		logger.debug(response.toString(4));

		cubridHttpsURLConnection.closeConnection();

		this.token = null;

		return response;
	}

	public JSONObject doCreatedb(String dbname) {
		if (this.token == null) {
			JSONObject loginResponse = doLogin();

			if ("success".equals(checkStatus(loginResponse)) == false) {
				return null;
			}
		}

		CUBRIDHttpsURLConnection cubridHttpsURLConnection = new CUBRIDHttpsURLConnection();
		cubridHttpsURLConnection.openConnection();

		final String PAGESIZE_16K = "16384";
		final String NUM_PAGE_512M = "32768";
		final String CUBRID_DATABASES = "/home/youngjinj/CUBRID/databases";

		String token = this.token;
		String task = "createdb";
		String numpage = NUM_PAGE_512M;
		String pagesize = PAGESIZE_16K;
		String logpagesize = PAGESIZE_16K;
		String logsize = NUM_PAGE_512M;
		String genvolpath = CUBRID_DATABASES + "/" + dbname;
		String logvolpath = CUBRID_DATABASES + "/" + dbname;
		String charset = "ko_KR.utf8";
		String overwrite_config_file = "YES";

		JSONObject request = new JSONObject().put("task", task).put("token", token).put("dbname", dbname)
				.put("numpage", numpage).put("pagesize", pagesize).put("logpagesize", logpagesize)
				.put("logsize", logsize).put("genvolpath", genvolpath).put("logvolpath", logvolpath)
				.put("charset", charset).put("overwrite_config_file", overwrite_config_file);
		logger.debug(request.toString(4));

		cubridHttpsURLConnection.sendRequest(request);

		JSONObject response = cubridHttpsURLConnection.recvResponse();
		logger.debug(response.toString(4));

		cubridHttpsURLConnection.closeConnection();

		return response;
	}

	public JSONObject doDeletedb(String dbname) {
		if (this.token == null) {
			JSONObject loginResponse = doLogin();

			if ("success".equals(checkStatus(loginResponse)) == false) {
				return null;
			}
		}

		CUBRIDHttpsURLConnection cubridHttpsURLConnection = new CUBRIDHttpsURLConnection();
		cubridHttpsURLConnection.openConnection();

		String token = this.token;
		String task = "deletedb";
		String delbackup = "y";

		JSONObject request = new JSONObject().put("task", task).put("token", token).put("dbname", dbname)
				.put("delbackup", delbackup);
		logger.debug(request.toString(4));

		cubridHttpsURLConnection.sendRequest(request);

		JSONObject response = cubridHttpsURLConnection.recvResponse();
		logger.debug(response.toString(4));

		cubridHttpsURLConnection.closeConnection();

		return response;
	}

	public JSONObject doAddvoldb(String dbname, String volname, String purpose, String numberofpages,
			String size_need_mb) {
		if (this.token == null) {
			JSONObject loginResponse = doLogin();

			if ("success".equals(checkStatus(loginResponse)) == false) {
				return null;
			}
		}

		CUBRIDHttpsURLConnection cubridHttpsURLConnection = new CUBRIDHttpsURLConnection();
		cubridHttpsURLConnection.openConnection();

		String token = this.token;
		String task = "addvoldb";

		JSONObject request = new JSONObject().put("task", task).put("token", token).put("dbname", dbname)
				.put("volname", volname).put("purpose", purpose).put("numberofpages", numberofpages)
				.put("size_need_mb", size_need_mb);
		logger.debug(request.toString(4));

		cubridHttpsURLConnection.sendRequest(request);

		JSONObject response = cubridHttpsURLConnection.recvResponse();
		logger.debug(response.toString(4));

		cubridHttpsURLConnection.closeConnection();

		return response;
	}

	public JSONObject doStartdb(String dbname) {
		if (this.token == null) {
			JSONObject loginResponse = doLogin();

			if ("success".equals(checkStatus(loginResponse)) == false) {
				return null;
			}
		}

		CUBRIDHttpsURLConnection cubridHttpsURLConnection = new CUBRIDHttpsURLConnection();
		cubridHttpsURLConnection.openConnection();

		String token = this.token;
		String task = "startdb";

		JSONObject request = new JSONObject().put("task", task).put("token", token).put("dbname", dbname);
		logger.debug(request.toString(4));

		cubridHttpsURLConnection.sendRequest(request);

		JSONObject response = cubridHttpsURLConnection.recvResponse();
		logger.debug(response.toString(4));

		cubridHttpsURLConnection.closeConnection();

		return response;
	}

	public String doStartinfo(String dbname) {
		if (this.token == null) {
			JSONObject loginResponse = doLogin();

			if ("success".equals(checkStatus(loginResponse)) == false) {
				return null;
			}
		}

		CUBRIDHttpsURLConnection cubridHttpsURLConnection = new CUBRIDHttpsURLConnection();
		cubridHttpsURLConnection.openConnection();

		String token = this.token;
		String task = "startinfo";

		JSONObject request = new JSONObject().put("task", task).put("token", token);
		logger.debug(request.toString(4));

		cubridHttpsURLConnection.sendRequest(request);

		JSONObject response = cubridHttpsURLConnection.recvResponse();
		logger.debug(response.toString(4));

		cubridHttpsURLConnection.closeConnection();

		// check start
		Iterator<Object> iterActivelist = response.getJSONArray("activelist").iterator();
		while (iterActivelist.hasNext()) {
			JSONArray activearray = ((JSONObject) iterActivelist.next()).getJSONArray("active");

			Iterator<Object> iterActive = activearray.iterator();
			while (iterActive.hasNext()) {
				JSONObject active = (JSONObject) iterActive.next();

				if (active.get("dbname") != null && dbname.equals(active.get("dbname"))) {
					return "active";
				}
			}
		}

		// check create
		Iterator<Object> iterDblist = response.getJSONArray("dblist").iterator();
		while (iterDblist.hasNext()) {
			JSONArray activearray = ((JSONObject) iterDblist.next()).getJSONArray("dbs");

			Iterator<Object> iterDbs = activearray.iterator();
			while (iterDbs.hasNext()) {
				JSONObject active = (JSONObject) iterDbs.next();

				if (active.get("dbname") != null && dbname.equals(active.get("dbname"))) {
					return "create";
				}
			}
		}

		return null;
	}

	public JSONObject doStopdb(String dbname) {
		if (this.token == null) {
			JSONObject loginResponse = doLogin();

			if ("success".equals(checkStatus(loginResponse)) == false) {
				return null;
			}
		}

		CUBRIDHttpsURLConnection cubridHttpsURLConnection = new CUBRIDHttpsURLConnection();
		cubridHttpsURLConnection.openConnection();

		String token = this.token;
		String task = "stopdb";

		JSONObject request = new JSONObject().put("task", task).put("token", token).put("dbname", dbname);
		logger.debug(request.toString(4));

		cubridHttpsURLConnection.sendRequest(request);

		JSONObject response = cubridHttpsURLConnection.recvResponse();
		logger.debug(response.toString(4));

		cubridHttpsURLConnection.closeConnection();

		return response;
	}

	public JSONObject doCreateuser(String dbname, String dbapass, String username) {
		String userpass = getRandomPassword();

		return doCreateuser(dbname, dbapass, username, userpass);
	}

	public JSONObject doCreateuser(String dbname, String dbapass, String username, String userpass) {
		if (this.token == null) {
			JSONObject loginResponse = doLogin();

			if ("success".equals(checkStatus(loginResponse)) == false) {
				return null;
			}
		}

		CUBRIDHttpsURLConnection cubridHttpsURLConnection = new CUBRIDHttpsURLConnection();
		cubridHttpsURLConnection.openConnection();

		String token = this.token;
		String task = "createuser";

		JSONObject request = new JSONObject().put("task", task).put("token", token).put("dbname", dbname)
				.put("username", username).put("userpass", userpass).put("_DBPASSWD", dbapass);
		logger.debug(request.toString(4));

		cubridHttpsURLConnection.sendRequest(request);

		JSONObject response = cubridHttpsURLConnection.recvResponse();
		logger.debug(response.toString(4));

		cubridHttpsURLConnection.closeConnection();

		return response;
	}
	
	public JSONObject doDeleteuser(String dbname, String dbapass, String username) {
		if (this.token == null) {
			JSONObject loginResponse = doLogin();

			if ("success".equals(checkStatus(loginResponse)) == false) {
				return null;
			}
		}

		CUBRIDHttpsURLConnection cubridHttpsURLConnection = new CUBRIDHttpsURLConnection();
		cubridHttpsURLConnection.openConnection();

		String token = this.token;
		String task = "deleteuser";

		JSONObject request = new JSONObject().put("task", task).put("token", token).put("dbname", dbname)
				.put("username", username).put("_DBPASSWD", dbapass);
		logger.debug(request.toString(4));

		cubridHttpsURLConnection.sendRequest(request);

		JSONObject response = cubridHttpsURLConnection.recvResponse();
		logger.debug(request.toString(4));

		cubridHttpsURLConnection.closeConnection();

		return response;
	}

	public JSONObject doUserinfo(String dbname, String dbapass) {
		if (this.token == null) {
			JSONObject loginResponse = doLogin();

			if ("success".equals(checkStatus(loginResponse)) == false) {
				return null;
			}
		}

		CUBRIDHttpsURLConnection cubridHttpsURLConnection = new CUBRIDHttpsURLConnection();
		cubridHttpsURLConnection.openConnection();

		String token = this.token;
		String task = "userinfo";

		JSONObject request = new JSONObject().put("task", task).put("token", token).put("dbname", dbname)
				.put("_DBPASSWD", dbapass);
		logger.debug(request.toString(4));

		cubridHttpsURLConnection.sendRequest(request);

		JSONObject response = cubridHttpsURLConnection.recvResponse();
		logger.debug(response.toString(4));

		cubridHttpsURLConnection.closeConnection();

		return response;
	}

	public JSONObject doUpdateuserDBAPassword(String dbname) {
		String userpass = getRandomPassword();

		return doUpdateuserDBAPassword(dbname, userpass);
	}

	public JSONObject doUpdateuserDBAPassword(String dbname, String userpass) {
		if (this.token == null) {
			JSONObject loginResponse = doLogin();

			if ("success".equals(checkStatus(loginResponse)) == false) {
				return null;
			}
		}

		CUBRIDHttpsURLConnection cubridHttpsURLConnection = new CUBRIDHttpsURLConnection();
		cubridHttpsURLConnection.openConnection();

		String token = this.token;
		String task = "updateuser";

		String username = "dba";

		JSONObject request = new JSONObject().put("task", task).put("token", token).put("dbname", dbname)
				.put("username", username).put("userpass", userpass);
		logger.debug(request.toString(4));

		cubridHttpsURLConnection.sendRequest(request);

		JSONObject response = cubridHttpsURLConnection.recvResponse();
		logger.debug(response.toString(4));

		cubridHttpsURLConnection.closeConnection();

		return response;
	}

}
