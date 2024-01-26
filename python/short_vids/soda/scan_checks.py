from soda.scan import Scan 
from datetime import datetime


scan = Scan()


# Add the data source's name  
scan.set_data_source_name("my_business_info")


# Add configuration.yml file
scan.add_configuration_yaml_file("configuration.yml")

# Stamp the scan with a scan date
scan_date = datetime.today()
scan.add_variables({"date": scan_date})


# Add the checks.yml file
scan.add_sodacl_yaml_file("checks.yml")



# Run the scan
scan.execute()
scan.assert_no_error_logs()
scan.assert_no_checks_fail()


# Evaluate the scan results
scan.get_scan_results()