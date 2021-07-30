package org.openpaas.servicebroker.cubrid.service.impl;

import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

import org.json.JSONObject;
import org.openpaas.servicebroker.model.CreateServiceInstanceRequest;
import org.openpaas.servicebroker.model.ServiceInstance;
import org.openpaas.servicebroker.model.ServiceInstanceBinding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Service;

import com.google.common.hash.Hashing;

@Service
public class CUBRIDAdminService {

	private Logger logger = LoggerFactory.getLogger(CUBRIDAdminService.class);

	/* service_instance_info */
	private final String CREATE_SERVICE_INSTANCE_INFO = "/* CREATE_SERVICE_INSTANCE_INFO */ CREATE TABLE [service_instance_info] ([service_instance_id] VARCHAR, [service_definition_id] VARCHAR, [plan_id] VARCHAR, [organization_guid] VARCHAR, [space_guid] VARCHAR, [updateDateTime] DATETIME DEFAULT SYSDATETIME, CONSTRAINT [pk_service_instance_info_service_instance_id] PRIMARY KEY([service_instance_id])) REUSE_OID";
	
	/* service_bind_info */	
	private final String CREATE_SERVICE_INSTANCE_BIND_INFO = "/* CREATE_SERVICE_INSTANCE_BIND_INFO */ CREATE TABLE [service_instance_bind_info] ([service_instance_bind_id] VARCHAR, [service_instance_id] VARCHAR, [application_id] VARCHAR, [username] VARCHAR, [updateDateTime] DATETIME DEFAULT SYSDATETIME, CONSTRAINT [pk_service_bind_info_service_instance_bind_id] PRIMARY KEY([service_instance_bind_id])) REUSE_OID";
	
	/* credentials_info */
	private final String CREATE_CREDENTIALS_INFO = "/* CREATE_CREDENTIALS_INFO */ CREATE TABLE [credentials_info] ([service_instance_id] VARCHAR, [service_instance_bind_id] VARCHAR, [name] VARCHAR, [hostname] VARCHAR, [port] VARCHAR, [username] VARCHAR, [password] VARCHAR, [uri] VARCHAR, [jdbcurl] VARCHAR, [updateDateTime] DATETIME DEFAULT SYSDATETIME, CONSTRAINT [pk_credentials_info_name_username] PRIMARY KEY([service_instance_id], [service_instance_bind_id])) REUSE_OID";
	
	/* findServiceInstanceInfo */
	private final String SELECT_SERVICE_INSTANCE_INFO = "/* SELECT_SERVICE_INSTANCE_INFO */ SELECT [service_instance_id], [Service_definition_id], [plan_id], [organization_guid], [space_guid] FROM [service_instance_info] WHERE [service_instance_id] = ?";

	/* save */
	private final String INSERT_SERVICE_INSTANCE_INFO = "/* INSERT_SERVICE_INSTANCE_INFO */ INSERT INTO [service_instance_info] ([service_instance_id], [service_definition_id], [plan_id], [organization_guid], [space_guid]) VALUES (?, ?, ?, ?, ?)";
	
	/* deleteServiceInstanceInfo */
	private final String DELETE_SERVICE_INSTANCE_INFO = "/* DELETE_SERVICE_INSTANCE_INFO */ DELETE FROM [service_instance_info] WHERE [service_instance_id] = ?";

	/* findServiceBindInfo */
	private final String SELECT_SERVICE_INSTANCE_BIND_INFO = "/* SELECT_SERVICE_INSTANCE_BIND_INFO */ SELECT [service_instance_bind_id], [service_instance_id], [application_id] FROM [service_instance_bind_info] WHERE [service_instance_bind_id] = ?";
	
	/* saveBind */
	private final String INSERT_SERVICE_INSTANCE_BIND_INFO = "/* INSERT_SERVICE_INSTANCE_BIND_INFO */ INSERT INTO [service_instance_bind_info] ([service_instance_bind_id], [service_instance_id], [application_id], [username]) VALUES (?, ?, ?, ?)";
	
	/* deleteServiceBindInfo */
	private final String DELETE_SERVICE_INSTANCE_BIND_INFO = "/* DELETE_SERVICE_INSTANCE_BIND_INFO */ DELETE FROM [service_instance_bind_info] WHERE [service_instance_bind_id] = ?";

	/* isExistsServiceBind */
	private final String SELECT_BIND_COUNT_SERVICE_INSTANCE_ID = "/* SELECT_BIND_COUNT_SERVICE_INSTANCE_ID */ SELECT count([service_instance_bind_id]) FROM [service_instance_bind_info] WHERE [service_instance_id] = ?";

	/* deleteServiceBind */
	private final String DELETE_BIND_SERVICE_INSTANCE_ID = "/* DELETE_BIND_SERVICE_INSTANCE_ID */ DELETE FROM [service_instance_bind_info] WHERE [service_instance_id] = ?";
	
	/* getCredentialsInfo */
	private final String SELECT_CREDENTIALS_INFO = "/* SELECT_CREDENTIALS_INFO */ SELECT [service_instance_id], [service_instance_bind_id], [name], [hostname], [port], [username], [password], [uri], [jdbcurl] FROM [credentials_info] WHERE [service_instance_bind_id] = ?";
	
	/* save */
	private final String INSERT_CREDENTIALS_INFO = "/* INSERT_CREDENTIALS_INFO */ INSERT INTO [credentials_info] ([service_instance_id], [service_instance_bind_id], [name], [hostname], [port], [username], [password], [uri], [jdbcurl]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

	/* deleteServiceInstanceInfo */
	private final String DELETE_CREDENTIALS_INFO = "/* DELETET_CREDENTIALS_INFO */ DELETE FROM [credentials_info] WHERE [service_instance_id] = ?";

	/* deleteServiceInstanceInfo */
	private final String DELETE_BIND_CREDENTIALS_INFO = "/* DELETET_CREDENTIALS_INFO */ DELETE FROM [credentials_info] WHERE [service_instance_bind_id] = ?";
	
	
	private final String SELECT_OWNER_OBJECT = "/* SELECT_OWNER_OBJECT */ SELECT DECODE ([class_type], 'CLASS', 'TABLE', 'VCLASS', 'VIEW') AS [object_type], [class_name] AS [name] FROM [db_class] WHERE [owner_name] = USER AND [class_type] IN ('CLASS', 'VCLASS') AND [is_system_class] = 'NO'"
			+ " UNION ALL SELECT 'SERIAL' AS [object_type], [name] AS [name] FROM [db_serial] WHERE [owner].[name] = USER AND [class_name] IS NULL"
			+ " UNION ALL SELECT [sp_type] AS [object_type], [sp_name] AS [name] FROM [db_stored_procedure] WHERE [owner] = USER AND [sp_type] IN ('PROCEDURE', 'FUNCTION')";

	@Autowired
	private JdbcTemplate jdbcTemplate;
	
	public ServiceInstance findServiceInstanceInfo(String serviceInstanceId) {
		try {
			return jdbcTemplate.queryForObject(SELECT_SERVICE_INSTANCE_INFO, new RowMapper<ServiceInstance>() {
				@Override
				public ServiceInstance mapRow(ResultSet rs, int rowNum) throws SQLException {
					CreateServiceInstanceRequest createServiceInstanceRequest = new CreateServiceInstanceRequest();

					createServiceInstanceRequest.withServiceInstanceId(rs.getString("service_instance_id"));
					createServiceInstanceRequest.setServiceDefinitionId(rs.getString("Service_definition_id"));
					createServiceInstanceRequest.setPlanId(rs.getString("plan_id"));
					createServiceInstanceRequest.setOrganizationGuid(rs.getString("organization_guid"));
					createServiceInstanceRequest.setSpaceGuid(rs.getString("space_guid"));

					return new ServiceInstance(createServiceInstanceRequest);
				}
			}, serviceInstanceId);
		} catch (DataAccessException e) {
			// Not error.
			logger.info("Service instance does not exist. : " + serviceInstanceId);

			return null;
		}
	}
	
	public void save(ServiceInstance serviceInstance) {
		try {
			this.jdbcTemplate.update(INSERT_SERVICE_INSTANCE_INFO,
					new Object[] { serviceInstance.getServiceInstanceId(), serviceInstance.getServiceDefinitionId(),
							serviceInstance.getPlanId(), serviceInstance.getOrganizationGuid(),
							serviceInstance.getSpaceGuid() });
		} catch (DataAccessException e) {
			logger.error(e.getLocalizedMessage(), e);
		}
		
		String serviceInstanceId = serviceInstance.getServiceInstanceId();
		 
		Map<String, Object> credentials = createCredentialsInfo(serviceInstanceId, null);
		
		try {
			this.jdbcTemplate.update(INSERT_CREDENTIALS_INFO,
					new Object[] { serviceInstanceId, "", credentials.get("name"),
							credentials.get("hostname"), credentials.get("port"), credentials.get("username"),
							credentials.get("password"), credentials.get("uri"), credentials.get("jdbcUrl") });
		} catch (DataAccessException e) {
			logger.error(e.getLocalizedMessage(), e);
		}
	}

	public void deleteServiceInstanceInfo(String serviceInstanceId) {
		try {
			this.jdbcTemplate.update(DELETE_SERVICE_INSTANCE_INFO, new Object[] { serviceInstanceId });
		} catch (DataAccessException e) {
			logger.error(e.getLocalizedMessage(), e);
		}

		try {
			this.jdbcTemplate.update(DELETE_CREDENTIALS_INFO, new Object[] { serviceInstanceId });
		} catch (DataAccessException e) {
			logger.error(e.getLocalizedMessage(), e);
		}
	}
	
	public ServiceInstanceBinding findServiceBindInfo(String serviceInstanceBindId) {
		try {
			return jdbcTemplate.queryForObject(SELECT_SERVICE_INSTANCE_BIND_INFO,
					new RowMapper<ServiceInstanceBinding>() {
						@Override
						public ServiceInstanceBinding mapRow(ResultSet rs, int rowNum) throws SQLException {
							return new ServiceInstanceBinding(rs.getString("service_instance_bind_id"),
									rs.getString("service_instance_id"), getCredentialsInfo(serviceInstanceBindId), "",
									rs.getString("application_id"));
						}
					}, serviceInstanceBindId);
		} catch (DataAccessException e) {
			// Not error.
			logger.error("Service instance bind does not exist. : " + serviceInstanceBindId);
			
			return null;
		}
	}
	
	public void saveBind(ServiceInstanceBinding serviceInstanceBinding) {
		try {
			this.jdbcTemplate.update(INSERT_SERVICE_INSTANCE_BIND_INFO,
					new Object[] { serviceInstanceBinding.getId(), serviceInstanceBinding.getServiceInstanceId(),
							serviceInstanceBinding.getAppGuid(),
							serviceInstanceBinding.getCredentials().get("username") });
		} catch (DataAccessException e) {
			logger.error(e.getLocalizedMessage(), e);
		}
		
		String serviceInstanceId = serviceInstanceBinding.getServiceInstanceId();
		String serviceInstanceBindId = serviceInstanceBinding.getId();
		
		Map<String, Object> credentials = serviceInstanceBinding.getCredentials();
		
		try {
			this.jdbcTemplate.update(INSERT_CREDENTIALS_INFO,
					new Object[] { serviceInstanceId, serviceInstanceBindId, credentials.get("name"),
							credentials.get("hostname"), credentials.get("port"), credentials.get("username"),
							credentials.get("password"), credentials.get("uri"), credentials.get("jdbcUrl") });
		} catch (DataAccessException e) {
			logger.error(e.getLocalizedMessage(), e);
		}
	}
	
	public void deleteServiceBindInfo(String serviceInstanceBindId) {
		try {
			this.jdbcTemplate.update(DELETE_SERVICE_INSTANCE_BIND_INFO, new Object[] { serviceInstanceBindId });
		} catch (DataAccessException e) {
			logger.error(e.getLocalizedMessage(), e);
		}
		
		try {
			this.jdbcTemplate.update(DELETE_BIND_CREDENTIALS_INFO, new Object[] { serviceInstanceBindId });
		} catch (DataAccessException e) {
			logger.error(e.getLocalizedMessage(), e);
		}
	}
	
	public boolean isExistsServiceBind(String serviceInstanceId) {
		Integer count = 0;
		try {
			count = jdbcTemplate.queryForObject(SELECT_BIND_COUNT_SERVICE_INSTANCE_ID, Integer.class, serviceInstanceId);
		} catch (DataAccessException e) {
			logger.error(e.getLocalizedMessage(), e);
		}

		return count > 0 ? true : false;
	}
	
	public void deleteServiceBind(String serviceInstanceId) {
		try {
			this.jdbcTemplate.update(DELETE_BIND_SERVICE_INSTANCE_ID, new Object[] { serviceInstanceId });
		} catch (DataAccessException e) {
			logger.error(e.getLocalizedMessage(), e);
		}
	}
	
	public Map<String, Object> createCredentialsInfo(String serviceInstanceId, String serviceInstanceBindId) {
		String dbname = getDbname(serviceInstanceId);
		
		String username = "dba";
		String password = getDbaPassword(serviceInstanceId);
		
		if (serviceInstanceBindId != null) {
			username = getUsername(serviceInstanceBindId);
			password = getRandomPassword();
		}
		
		StringBuilder uri = new StringBuilder();
		uri.append("cubrid").append(":");
		uri.append(CUBRIDHttpsURLConnection.CM_IP).append(":");
		uri.append("33000").append(":");
		uri.append(dbname).append(":");
		uri.append(username).append(":");
		uri.append(password).append(":");
		
		StringBuilder jdbcUrl = new StringBuilder();
		jdbcUrl.append("jdbc").append(":");
		jdbcUrl.append(uri.toString());
		
		Map<String, Object> credentials = new HashMap<String, Object>();
		credentials.put("name", dbname);
		credentials.put("hostname", CUBRIDHttpsURLConnection.CM_IP);
		credentials.put("port", 33000);
		credentials.put("username", username);
		credentials.put("password", password);
		credentials.put("uri", uri.toString());
		credentials.put("jdbcUrl", jdbcUrl);
		
		return credentials;
	}
	
	public String getErrorMessage(String request, String serviceInstanceId, String dbname, JSONObject response) {
		StringJoiner message = new StringJoiner(" ");

		message.add("[").add(request).add("-").add(serviceInstanceId + "(" + dbname + ")").add("]");

		if (response != null && response.get("task") != null) {
			message.add(response.get("task").toString()).add(":");
		}

		if (response != null && response.get("status") != null) {
			message.add(response.get("status").toString());
		}

		if (response != null && response.get("note") != null) {
			message.add("-").add(response.get("note").toString());
		}

		return message.toString();
	}
	
	public String getDbname(String serviceInstanceId) {
		return "db_" + Hashing.sha256().hashString(serviceInstanceId, StandardCharsets.UTF_8).toString().substring(0, 12);
	}
	
	public String getDbaPassword(String serviceInstanceId) {
		return Hashing.sha256().hashString("dba" + serviceInstanceId, StandardCharsets.UTF_8).toString().substring(0, 15);
	}
	
	public String getUsername(String serviceInstanceBindId) {
		return "user_" + Hashing.sha256().hashString(serviceInstanceBindId, StandardCharsets.UTF_8).toString().substring(0, 10);
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
			logger.error(e.getLocalizedMessage(), e);
		}

		for (int i = 0; i < length; i++) {
			int index = secureRandom.nextInt(AlphabetNumberSpecialCharacter.length());
			password.append(AlphabetNumberSpecialCharacter.charAt(index));
		}

		return password.toString();
	}

	public Map<String, Object> getCredentialsInfo(String serviceInstanceBindId) {
		Map<String, Object> credentials = null;

		try {
			credentials = jdbcTemplate.queryForObject(SELECT_CREDENTIALS_INFO, new RowMapper<Map<String, Object>>() {
				@Override
				public Map<String, Object> mapRow(ResultSet rs, int rowNum) throws SQLException {
					Map<String, Object> credentials = new HashMap<String, Object>();

					credentials.put("name", rs.getString("name"));
					credentials.put("hostname", rs.getString("hostname"));
					credentials.put("port", rs.getString("port"));
					credentials.put("username", rs.getString("username"));
					credentials.put("password", rs.getString("password"));
					credentials.put("uri", rs.getString("uri"));
					credentials.put("jdbcurl", rs.getString("jdbcurl"));

					return credentials;
				}
			}, serviceInstanceBindId);
		} catch (DataAccessException e) {
			logger.error(e.getLocalizedMessage(), e);
		}

		return credentials;
	}
	
	public void dropOwnerObject(String serviceInstanceBindId) {
		Map<String, Object> credentials = getCredentialsInfo(serviceInstanceBindId);
	
		DriverManagerDataSource dataSource = new DriverManagerDataSource();
		dataSource.setDriverClassName("cubrid.jdbc.driver.CUBRIDDriver");
		dataSource.setUrl(String.valueOf(credentials.get("jdbcurl")));
		dataSource.setUsername(String.valueOf(credentials.get("username")));
		dataSource.setPassword(String.valueOf(credentials.get("password")));
		
		JdbcTemplate instanceJdbcTemplate = new JdbcTemplate(dataSource);
		
		List<Map<String, Object>> ownerObject = instanceJdbcTemplate.queryForList(SELECT_OWNER_OBJECT);
		
		Iterator<Map<String, Object>> ownerObjectIter = ownerObject.iterator();
		while (ownerObjectIter.hasNext()) {
			Map<String, Object> ownerObjectRow = ownerObjectIter.next();
			
			String objectType = ownerObjectRow.get("object_type").toString();
			String name = ownerObjectRow.get("name").toString();
			
			instanceJdbcTemplate.execute("DROP " + objectType + " " + name);
		}
	}
}
