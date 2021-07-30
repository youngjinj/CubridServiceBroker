package org.openpaas.servicebroker.cubrid.config;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.PropertySources;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

@Configuration
@PropertySources({ @PropertySource("classpath:datasource.properties"), })
public class CUBRIDConfig {

	@Autowired
	private Environment env;

	@Bean
	public DataSource dataSource() {
		DriverManagerDataSource dataSource = new DriverManagerDataSource();
		dataSource.setDriverClassName(this.env.getRequiredProperty("jdbc.driver"));
		dataSource.setUrl(this.env.getRequiredProperty("jdbc.url"));
		dataSource.setUsername(this.env.getRequiredProperty("jdbc.userName"));
		dataSource.setPassword(this.env.getRequiredProperty("jdbc.password"));
		return dataSource;
	}
}