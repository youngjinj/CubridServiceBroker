package org.openpaas.servicebroker.cubrid.service.impl;

import java.util.Map;

import org.json.JSONObject;
import org.openpaas.servicebroker.cubrid.exception.CUBRIDHttpsURLServiceException;
import org.openpaas.servicebroker.exception.ServiceBrokerException;
import org.openpaas.servicebroker.exception.ServiceInstanceBindingExistsException;
import org.openpaas.servicebroker.model.CreateServiceInstanceBindingRequest;
import org.openpaas.servicebroker.model.DeleteServiceInstanceBindingRequest;
import org.openpaas.servicebroker.model.ServiceInstance;
import org.openpaas.servicebroker.model.ServiceInstanceBinding;
import org.openpaas.servicebroker.service.ServiceInstanceBindingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class CubridServiceInstanceBindingService implements ServiceInstanceBindingService {
	
	private static final Logger logger = LoggerFactory.getLogger(CubridServiceInstanceBindingService.class);
	
	@Autowired
	private CUBRIDAdminService cubridAdminService;
	
	@Autowired
	private CUBRIDHttpsURLService cubridHttpsURLService = null;

	@Autowired
	public CubridServiceInstanceBindingService(CUBRIDAdminService cubridAdminService, CUBRIDHttpsURLService cubridHttpsURLService) {
		this.cubridAdminService = cubridAdminService;
		this.cubridHttpsURLService = cubridHttpsURLService;
	}

	@Override
	public ServiceInstanceBinding createServiceInstanceBinding(CreateServiceInstanceBindingRequest createServiceInstanceBindingRequest) throws ServiceInstanceBindingExistsException, ServiceBrokerException {
		
		ServiceInstanceBinding findServiceInstanceBinding = cubridAdminService.findServiceBindInfo(createServiceInstanceBindingRequest.getBindingId());
		
		if (findServiceInstanceBinding != null) {
			if (createServiceInstanceBindingRequest.getBindingId().equals(findServiceInstanceBinding.getId())
					&& createServiceInstanceBindingRequest.getServiceInstanceId().equals(findServiceInstanceBinding.getServiceInstanceId())
					&& createServiceInstanceBindingRequest.getAppGuid().equals(findServiceInstanceBinding.getAppGuid())) {
				logger.error("Service instance bind already exists. : " + findServiceInstanceBinding.getId());
				
				findServiceInstanceBinding.setHttpStatusOK();
				
				return findServiceInstanceBinding;
			} else {
				logger.error("Service instance bind information does not match. : " + findServiceInstanceBinding.getId());
				
				throw new ServiceInstanceBindingExistsException(findServiceInstanceBinding);
			}
		} else { // findServiceInstanceBinding == null
			logger.info("Start binding service instance. : " + createServiceInstanceBindingRequest.getBindingId());
			
			ServiceInstance findServiceInstance = cubridAdminService.findServiceInstanceInfo(createServiceInstanceBindingRequest.getServiceInstanceId());
			
			String serviceInstanceId = createServiceInstanceBindingRequest.getServiceInstanceId();
			String serviceInstanceBindId = createServiceInstanceBindingRequest.getBindingId();
			
			if (findServiceInstance != null) { // (findServiceInstanceBinding == null) && (findServiceInstance != null)
				Map<String, Object> credentials = cubridAdminService.createCredentialsInfo(serviceInstanceId,
						serviceInstanceBindId);

				ServiceInstanceBinding createServiceInstanceBinding = new ServiceInstanceBinding(serviceInstanceBindId,
						serviceInstanceId, credentials, "", createServiceInstanceBindingRequest.getAppGuid());

				String dbname = String.valueOf(credentials.get("name"));
				String dbapass = cubridAdminService.getDbaPassword(serviceInstanceId);
				String username = String.valueOf(credentials.get("username"));
				String password = String.valueOf(credentials.get("password"));
				
				JSONObject response = null;

				logger.info("Create user. : " + serviceInstanceBindId + " (" + dbname + " - " + username + ")");
				response = cubridHttpsURLService.doCreateuser(dbname, dbapass, username, password);
				if (response != null && response.get("status") != null && ("success".equals(response.get("status").toString()) == false)) {
					logger.error("User creation failed. : " + serviceInstanceBindId + " (" + dbname + " - " + username + ")");
					
					throw new CUBRIDHttpsURLServiceException(cubridAdminService.getErrorMessage("service-bind: create bind-user", serviceInstanceId, dbname, response));
				}
					
				if (response != null && response.get("status") != null && ("success".equals(response.get("status").toString()) == true)) {
					logger.info("Save service instance bind information. : " + serviceInstanceBindId + " (" + dbname + " - " + username + ")");
					
					cubridAdminService.saveBind(createServiceInstanceBinding);
				}
				
				logger.info("Service instance binding complete. : " + serviceInstanceBindId);

				return createServiceInstanceBinding;
			} else { // (findServiceInstanceBinding == null) && (findServiceInstance == null)
				logger.error("Service instance does not exist. : " + serviceInstanceId);
				
				throw new ServiceBrokerException("Service instance does not exist. : " + serviceInstanceId);
			}
		}
	}
	
	@Override
	public ServiceInstanceBinding deleteServiceInstanceBinding(DeleteServiceInstanceBindingRequest deleteServiceInstanceBindingRequest) throws ServiceBrokerException {
		
		ServiceInstanceBinding findServiceInstanceBinding = cubridAdminService.findServiceBindInfo(deleteServiceInstanceBindingRequest.getBindingId());
		
		if (findServiceInstanceBinding != null) {
			logger.info("Start deleting service instance binds. : " + deleteServiceInstanceBindingRequest.getBindingId());
			
			ServiceInstance findServiceInstance = cubridAdminService.findServiceInstanceInfo(findServiceInstanceBinding.getServiceInstanceId());
			
			String serviceInstanceId = findServiceInstanceBinding.getServiceInstanceId();
			String serviceInstanceBindId = findServiceInstanceBinding.getId();
			
			if (findServiceInstance != null) {
				String dbname = cubridAdminService.getDbname(serviceInstanceId);
				String dbapass = cubridAdminService.getDbaPassword(serviceInstanceId);
				String username = cubridAdminService.getUsername(serviceInstanceBindId);	
				
				JSONObject response = null;
				
				logger.info("Delete user object. : " + serviceInstanceBindId + " (" + dbname + " - " + username + ")");
				cubridAdminService.dropOwnerObject(serviceInstanceBindId);
				
				logger.info("Delete user. : " + serviceInstanceBindId + " (" + dbname + " - " + username + ")");
				response = cubridHttpsURLService.doDeleteuser(dbname, dbapass, username);
				if (response != null && response.get("status") != null && ("success".equals(response.get("status").toString()) == false)) {
					logger.info("Failed to delete user. : " + serviceInstanceBindId + " (" + dbname + " - " + username + ")");
					
					throw new CUBRIDHttpsURLServiceException(cubridAdminService.getErrorMessage("service-unbind: delete bind-user", serviceInstanceId, dbname, response));
				}
				
				logger.info("Delete service instance bind information. : " + serviceInstanceBindId);
				cubridAdminService.deleteServiceBindInfo(serviceInstanceBindId);
				
				logger.info("Service instance bind deletion complete. : " + serviceInstanceBindId);
				
				return findServiceInstanceBinding;
			} else { // (findServiceInstanceBinding != null) && (findServiceInstance == null)
				logger.error("Service instance bind information does not match. : " + serviceInstanceBindId);
				
				logger.info("Delete invalid service instance bind. : " + serviceInstanceBindId);
				cubridAdminService.deleteServiceBindInfo(serviceInstanceBindId);
				
				logger.info("Invalid service instance bind deletion complete. : " + serviceInstanceBindId);
				
				return null;
			}
		} else { // findServiceInstanceBinding == null
			logger.error("Service instance bind does not exist. : " + deleteServiceInstanceBindingRequest.getBindingId());
			
			return null;
		}
	}
}
