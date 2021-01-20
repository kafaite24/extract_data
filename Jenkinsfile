import groovy.sql.Sql

pipeline {
    agent any
    stages {
       		
		
		stage('Create tables'){
			steps{
                script{
					
					
					
					sqlconnection().eachRow("SELECT COUNT(*) as output FROM dbc.TABLES WHERE TABLENAME = 'departments_landing' and databasename='KH255051'") { row ->
					def table= "$row.output"
			
					if("${table}"=="1"){
						sqlconnection().execute'''
							delete from departments_landing
						'''
						sqlconnection().execute'''
							Alter table departments_landing
							drop DateAdded
						'''
					}
					else{
						sqlconnection().execute'''
							CREATE Multiset TABLE KH255051.departments_landing (
								department_id INT NOT NULL PRIMARY KEY,
								department_name VARCHAR (30) NOT NULL,
								location VARCHAR (100) DEFAULT NULL);
							'''
					}
					}
					
					sqlconnection().eachRow("SELECT COUNT(*) as output FROM dbc.TABLES WHERE TABLENAME = 'jobs_landing' and databasename='KH255051'") { row ->
					def table= "$row.output"
			
					if("${table}"=="1"){
						sqlconnection().execute'''
							delete from jobs_landing
						'''
						sqlconnection().execute'''
							Alter table jobs_landing
							drop DateAdded
						'''
					}
					else{
						sqlconnection().execute'''
							CREATE Multiset TABLE KH255051.jobs_landing (
								job_id INT NOT NULL PRIMARY KEY,
								job_title VARCHAR (35) NOT NULL,
								min_salary DECIMAL (8, 2) DEFAULT NULL,
								max_salary DECIMAL (8, 2) DEFAULT NULL);
							'''}
					}
					
					sqlconnection().eachRow("SELECT COUNT(*) as output FROM dbc.TABLES WHERE TABLENAME = 'employees_landing' and databasename='KH255051'") { row ->
					def table= "$row.output"
				
					if("${table}"=="1"){
						sqlconnection().execute'''
							delete from employees_landing
						'''
						sqlconnection().execute'''
							Alter table employees_landing
							drop DateAdded
						'''
					}
					else{
						sqlconnection().execute'''
							CREATE Multiset TABLE KH255051.employees_landing (
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
					
					}}
				}
		}
		stage('load data in landing departments table'){
            steps{
                script{
					
					
					String csvFilePath = "${departments_csv_file_path}"
						
					String insert = "insert into departments_landing values(?,?,?)";
			
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
		stage('load data in landing jobs table'){
            steps{
                script{
					
					
					String csvFilePath = "${jobs_csv_file_path}"
						
					String insert = "insert into jobs_landing values(?,?,?,?)";
			
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
		stage('load data in landing employees table'){
            steps{
                script{
					
					
					String csvFilePath = "${staff_csv_file_path}"
						
					String insert = "insert into employees_landing values( ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			
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
			
		stage('load data in staging tables'){
            steps{
                script{
				
					sqlconnection().execute '''
						ALTER TABLE employees_landing
						ADD DateAdded TIMESTAMP(6);
					'''
					
					sqlconnection().execute '''
						ALTER TABLE departments_landing
						ADD DateAdded TIMESTAMP(6);
					'''
					
					sqlconnection().execute '''
						ALTER TABLE jobs_landing 
						ADD DateAdded TIMESTAMP(6);
					'''
					
					sqlconnection().execute '''
						update employees_landing
						set DateAdded= current_timestamp
					'''

					sqlconnection().execute '''
						update departments_landing
						set DateAdded= current_timestamp
					'''
					
					sqlconnection().execute '''
						update jobs_landing
						set DateAdded= current_timestamp
					'''
					
					sqlconnection().eachRow("SELECT COUNT(*) as output FROM dbc.TABLES WHERE TABLENAME = 'employees_staging' and databasename='KH255051'") { row ->
					def table= "$row.output"
			
					if("${table}"=="0"){
						
						sqlconnection().execute'''
							CREATE Multiset TABLE employees_staging AS
							(select * from employees_landing)
							with no data;
						'''
					}

					sqlconnection().execute'''
						insert into employees_staging
						select * from employees_landing
					'''
					}
					
					sqlconnection().eachRow("SELECT COUNT(*) as output FROM dbc.TABLES WHERE TABLENAME = 'departments_staging' and databasename='KH255051'") { row ->
					def table= "$row.output"
			
					if("${table}"=="0"){
						
						sqlconnection().execute'''
							CREATE Multiset TABLE departments_staging AS
							(select * from departments_landing)
							with no data;
						'''
					}
					
					sqlconnection().execute'''
						insert into departments_staging
						select * from departments_landing
					'''
					
					}
					
					sqlconnection().eachRow("SELECT COUNT(*) as output FROM dbc.TABLES WHERE TABLENAME = 'jobs_staging' and databasename='KH255051'") { row ->
					def table= "$row.output"
			
					
					if("${table}"=="0"){
						
						sqlconnection().execute'''
							CREATE Multiset TABLE jobs_staging AS
							(select * from jobs_landing)
							with no data;
						'''
					}

					sqlconnection().execute'''
						insert into jobs_staging
						select * from jobs_landing
					'''
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
