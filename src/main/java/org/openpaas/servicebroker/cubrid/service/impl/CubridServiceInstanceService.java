package org.openpaas.servicebroker.cubrid.service.impl;

import org.json.JSONObject;
import org.openpaas.servicebroker.cubrid.exception.CUBRIDHttpsURLServiceException;
import org.openpaas.servicebroker.exception.ServiceBrokerException;
import org.openpaas.servicebroker.exception.ServiceInstanceDoesNotExistException;
import org.openpaas.servicebroker.exception.ServiceInstanceExistsException;
import org.openpaas.servicebroker.exception.ServiceInstanceUpdateNotSupportedException;
import org.openpaas.servicebroker.model.CreateServiceInstanceRequest;
import org.openpaas.servicebroker.model.DeleteServiceInstanceRequest;
import org.openpaas.servicebroker.model.ServiceInstance;
import org.openpaas.servicebroker.model.UpdateServiceInstanceRequest;
import org.openpaas.servicebroker.service.ServiceInstanceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class CubridServiceInstanceService implements ServiceInstanceService {
	
	private static final Logger logger = LoggerFactory.getLogger(CubridServiceInstanceService.class);
	
	@Autowired
	private CUBRIDAdminService cubridAdminService = null;
	
	@Autowired
	private CUBRIDHttpsURLService cubridHttpsURLService = null;
	
	@Autowired
	public CubridServiceInstanceService(CUBRIDAdminService cubridAdminService, CUBRIDHttpsURLService cubridHttpsURLService) {
		this.cubridAdminService = cubridAdminService;
		this.cubridHttpsURLService = cubridHttpsURLService;
	}

	@Override
	public ServiceInstance createServiceInstance(CreateServiceInstanceRequest createServiceInstanceRequest) throws ServiceInstanceExistsException, ServiceBrokerException {

		ServiceInstance findServiceInstance = cubridAdminService.findServiceInstanceInfo(createServiceInstanceRequest.getServiceInstanceId());
		
		if (findServiceInstance != null) {
			if (createServiceInstanceRequest.getServiceInstanceId().equals(findServiceInstance.getServiceInstanceId())
					&& createServiceInstanceRequest.getPlanId().equals(findServiceInstance.getPlanId())
					&& createServiceInstanceRequest.getServiceDefinitionId().equals(findServiceInstance.getServiceDefinitionId())) {
				logger.error("Service instance already exists. : " + createServiceInstanceRequest.getServiceInstanceId());
				
				findServiceInstance.setHttpStatusOK();
				
				return findServiceInstance;
			} else {
				logger.error("Service instance creation information does not match. : " + createServiceInstanceRequest.getServiceInstanceId());
				
				throw new ServiceInstanceExistsException(findServiceInstance);
			}
		} else { // findServiceInstance == null
			logger.info("Start creating service instance. : " + createServiceInstanceRequest.getServiceInstanceId());
			
			ServiceInstance createServiceInstance = new ServiceInstance(createServiceInstanceRequest);
			
			String serviceInstanceId = createServiceInstance.getServiceInstanceId();
			String dbname = cubridAdminService.getDbname(serviceInstanceId);
			String dbapass = cubridAdminService.getDbaPassword(serviceInstanceId);
			
			String status = cubridHttpsURLService.doStartinfo(dbname);
			JSONObject response = null;
			
			if (cubridAdminService.isExistsServiceBind(createServiceInstance.getServiceInstanceId())) {
				logger.info("Delete invalid service instance bind. : " + serviceInstanceId);
				
				cubridAdminService.deleteServiceBindInfo(createServiceInstance.getServiceInstanceId());
			}
			
			if ("active".equals(status)) {
				logger.info("stop invalid database. : " + serviceInstanceId + " (" + dbname + ")");
				
				response = cubridHttpsURLService.doStopdb(dbname);
				if (response != null && response.get("status") != null && ("success".equals(response.get("status").toString()) == false)) {
					logger.error("Failed to stop invalid database. : " + serviceInstanceId + " (" + dbname + ")");
					
					throw new CUBRIDHttpsURLServiceException(cubridAdminService.getErrorMessage("service-create: stop old-instance", serviceInstanceId, dbname, response));
				}
			}
			
			if ("create".equals(status) || "active".equals(status)) {
				logger.info("Delete invalid database. : " + serviceInstanceId + " (" + dbname + ")");
				
				response = cubridHttpsURLService.doDeletedb(dbname);
				if (response != null && response.get("status") != null && ("success".equals(response.get("status").toString()) == false)) {
					logger.error("Failed to delete invalid database. : " + serviceInstanceId + " (" + dbname + ")");
					
					throw new CUBRIDHttpsURLServiceException(cubridAdminService.getErrorMessage("service-create: delete old-instance", serviceInstanceId, dbname, response));
				}
			}
			
			logger.info("Create database. : " + serviceInstanceId + " (" + dbname + ")");
			response = cubridHttpsURLService.doCreatedb(dbname);
			if (response != null && response.get("status") != null && ("success".equals(response.get("status").toString()) == false)) {
				logger.error("Failed to create database. : " + serviceInstanceId + " (" + dbname + ")");
				
				throw new CUBRIDHttpsURLServiceException(cubridAdminService.getErrorMessage("service-create: create new-instance", serviceInstanceId, dbname, response));
			}
			
			response = cubridHttpsURLService.doStartdb(dbname);
			logger.info("Start database. : " + serviceInstanceId + " (" + dbname + ")");
			if (response != null && response.get("status") != null && ("success".equals(response.get("status").toString()) == false)) {
				logger.error("Failed to start database. : " + serviceInstanceId + " (" + dbname + ")");
				
				throw new CUBRIDHttpsURLServiceException(cubridAdminService.getErrorMessage("service-create: start new-instance", serviceInstanceId, dbname, response));
			}
			
			response = cubridHttpsURLService.doUpdateuserDBAPassword(dbname, dbapass);
			logger.info("Change dba password. : " + serviceInstanceId + " (" + dbname + ")");
			if (response != null && response.get("status") != null && ("success".equals(response.get("status").toString()) == false)) {
				logger.error("Failed to change dba password. : " + serviceInstanceId + " (" + dbname + ")");
				
				throw new CUBRIDHttpsURLServiceException(cubridAdminService.getErrorMessage("service-create: change dba-password", serviceInstanceId, dbname, response));
			}
			
			if (response != null && response.get("status") != null && ("success".equals(response.get("status").toString()) == true)) {
				logger.info("Save database creation information. : " + serviceInstanceId + " (" + dbname + ")");
				
				cubridAdminService.save(createServiceInstance);
			}
			
			logger.info("Service instance creation complete. : " + serviceInstanceId);
			
			return createServiceInstance;
		}
	}

	@Override
	public ServiceInstance deleteServiceInstance(DeleteServiceInstanceRequest deleteServiceInstanceRequest) throws ServiceBrokerException {
		
		ServiceInstance findServiceInstance = cubridAdminService.findServiceInstanceInfo(deleteServiceInstanceRequest.getServiceInstanceId());

		if (findServiceInstance != null) {
			logger.info("Start deleting service instance. : " + deleteServiceInstanceRequest.getServiceInstanceId());
			
			ServiceInstance deleteServiceInstance = new ServiceInstance(deleteServiceInstanceRequest);
			
			String serviceInstanceId = deleteServiceInstanceRequest.getServiceInstanceId();
			String dbname = cubridAdminService.getDbname(serviceInstanceId);
			
			String status = cubridHttpsURLService.doStartinfo(dbname);
			JSONObject response = null;
			
			if (cubridAdminService.isExistsServiceBind(deleteServiceInstance.getServiceInstanceId())) {
				logger.info("Delete invalid service instance bind. : " + serviceInstanceId);
				
				cubridAdminService.deleteServiceBindInfo(deleteServiceInstance.getServiceInstanceId());
			}
			
			if ("active".equals(status)) {
				logger.info("stop database. : " + serviceInstanceId + " (" + dbname + ")");
				
				response = cubridHttpsURLService.doStopdb(dbname);
				if (response != null && response.get("status") != null && ("success".equals(response.get("status").toString()) == false)) {
					logger.error("Failed to stop database. : " + serviceInstanceId + " (" + dbname + ")");
					
					throw new CUBRIDHttpsURLServiceException(cubridAdminService.getErrorMessage("service-delete: stop instance", serviceInstanceId, dbname, response));
				}
			}
			
			if ("create".equals(status) || "active".equals(status)) {
				logger.info("Delete database. : " + serviceInstanceId + " (" + dbname + ")");
				
				response = cubridHttpsURLService.doDeletedb(dbname);
				if (response != null && response.get("status") != null && ("success".equals(response.get("status").toString()) == false)) {
					logger.error("Failed to delete database. : " + serviceInstanceId + " (" + dbname + ")");
					
					throw new CUBRIDHttpsURLServiceException(cubridAdminService.getErrorMessage("service-delete: delete instance", serviceInstanceId, dbname, response));
				}
			}
			
			logger.info("Delete database creation information. : " + serviceInstanceId + " (" + dbname + ")");
			cubridAdminService.deleteServiceInstanceInfo(findServiceInstance.getServiceInstanceId());
			
			logger.info("Service instance deletion complete. : " + serviceInstanceId);
			
			return findServiceInstance;
		} else { // findServiceInstance == null
			logger.error("Service instance does not exist. : " + deleteServiceInstanceRequest.getServiceInstanceId());
			
			return null;
		}
	}

	@Override
	public ServiceInstance getServiceInstance(String serviceInstanceId) {
		return cubridAdminService.findServiceInstanceInfo(serviceInstanceId);
	}

	@Override
	public ServiceInstance updateServiceInstance(UpdateServiceInstanceRequest updateServiceInstanceRequest) throws ServiceInstanceUpdateNotSupportedException, ServiceBrokerException, ServiceInstanceDoesNotExistException {
		logger.info("Service instance (" + updateServiceInstanceRequest.getServiceInstanceId() + ") update. (Not supported.)");
		
		throw new ServiceInstanceUpdateNotSupportedException("Update of service instance is not supported.");
	}
}
