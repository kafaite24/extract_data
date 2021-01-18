import groovy.sql.Sql

pipeline {
    agent any
    stages {
       		
		
		stage('Create tables'){
			steps{
                script{
					
					sqlconnection().eachRow("SELECT COUNT(*) as output FROM dbc.TABLES WHERE TABLENAME = 'staff' and databasename='KH255051'") { row ->
					def table= "$row.output"
				
					if("${table}"=="1"){
						sqlconnection().execute'''
							drop table staff
						'''
					}}
					
					sqlconnection().eachRow("SELECT COUNT(*) as output FROM dbc.TABLES WHERE TABLENAME = 'departments' and databasename='KH255051'") { row ->
					def table= "$row.output"
			
					if("${table}"=="1"){
						sqlconnection().execute'''
							drop table departments
						'''
					}}
					
					sqlconnection().eachRow("SELECT COUNT(*) as output FROM dbc.TABLES WHERE TABLENAME = 'jobs' and databasename='KH255051'") { row ->
					def table= "$row.output"
			
					if("${table}"=="1"){
						sqlconnection().execute'''
							drop table jobs
						'''
					}}
					
					sqlconnection().execute'''
						CREATE TABLE KH255051.departments (
							department_id INT NOT NULL PRIMARY KEY,
							department_name VARCHAR (30) NOT NULL,
							location VARCHAR (100) DEFAULT NULL);
						'''
					

					sqlconnection().execute'''
						CREATE TABLE KH255051.jobs (
							job_id INT NOT NULL PRIMARY KEY,
							job_title VARCHAR (35) NOT NULL,
							min_salary DECIMAL (8, 2) DEFAULT NULL,
							max_salary DECIMAL (8, 2) DEFAULT NULL);
						'''
					
					
					sqlconnection().execute'''
						CREATE TABLE KH255051.staff (
							employee_id INT NOT NULL PRIMARY KEY,
							name_prefix VARCHAR (20) DEFAULT NULL,
							first_name VARCHAR (20) DEFAULT NULL,
							middle_initial VARCHAR (20) DEFAULT NULL,
							last_name VARCHAR (25) NOT NULL,
							gender VARCHAR (20) DEFAULT NULL,
							email VARCHAR (100) NOT NULL,
							father_name VARCHAR (100) DEFAULT NULL,
							mother_name VARCHAR (100) DEFAULT NULL,
							mother_maiden_name VARCHAR (100) DEFAULT NULL,
							dob DATE DEFAULT NULL,
							age DECIMAL (8, 2) DEFAULT NULL,
							weight DECIMAL (8, 2) DEFAULT NULL,
							hire_date DATE NOT NULL,
							half_of_joining VARCHAR (20) DEFAULT NULL,
							year_of_joining int DEFAULT NULL,
							month_of_joining int DEFAULT NULL,
							month_name_joining VARCHAR (20) DEFAULT NULL,
							age_in_company DECIMAL (8, 2) DEFAULT NULL,
							salary DECIMAL (8, 2) DEFAULT NULL,
							SSN VARCHAR (20) DEFAULT NULL,
							phone_number VARCHAR (20) DEFAULT NULL,
							country VARCHAR (20) DEFAULT NULL,
							city VARCHAR (20) DEFAULT NULL,
							state VARCHAR (20) DEFAULT NULL,
							region VARCHAR (20) DEFAULT NULL,
							zip_code INT DEFAULT NULL,
							username VARCHAR (100) DEFAULT NULL,
							passcode VARCHAR (100) DEFAULT NULL,
							job_id INT NOT NULL,
							manager_id INT DEFAULT NULL,
							department_id INT DEFAULT NULL);
						'''
				
					}
				}
		}
		stage('load data in departments table'){
            steps{
                script{
					
					
					String csvFilePath = "${departments_csv_file_path}"
						
					String insert = "insert into departments values(?,?,?)";
			
					BufferedReader lineReader = new BufferedReader(new FileReader(csvFilePath));
						
					def data= []
					String line = lineReader.readLine();
						
					while (line != null) {
						
						data = line.split(",");
						sqlconnection().execute insert,data
						line = lineReader.readLine();
						
					}

				}
			}}
		stage('load data in jobs table'){
            steps{
                script{
					
					
					String csvFilePath = "${jobs_csv_file_path}"
						
					String insert = "insert into jobs values(?,?,?,?)";
			
					BufferedReader lineReader = new BufferedReader(new FileReader(csvFilePath));
						
					def data= []
					String line = lineReader.readLine();
						
					while (line != null) {
						
						data = line.split(",");
						sqlconnection().execute insert,data
						line = lineReader.readLine();
						
					}

				}
			}}
		stage('load data in staff table'){
            steps{
                script{
					
					
					String csvFilePath = "${staff_csv_file_path}"
						
					String insert = "insert into staff values( ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			
					BufferedReader lineReader = new BufferedReader(new FileReader(csvFilePath));
						
					def data= []
					String line = lineReader.readLine();
						
					while (line != null) {
						
						data = line.split(",");
						sqlconnection().execute insert,data
						line = lineReader.readLine();
						
					}

				}
			}}
		}
	
}

@NonCPS
    def sqlconnection() {
        String URL = "jdbc:teradata://wbtlabserver.teradata.com/KH255051";
        String username = "KH255051";
        String password = "${dbc_password}";
        String driver= "com.teradata.jdbc.TeraDriver"
        
        echo "Building connection"
        def sqlconn = Sql.newInstance(URL, username, password, driver)
        return sqlconn
    }
