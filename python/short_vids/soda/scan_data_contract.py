from soda.contracts.data_contract_translator import DataContractTranslator
from soda.scan import Scan
import logging

# Read the data contract file as a Python str
with open("data_contract.yml") as f:
    data_contract_yaml_str: str = f.read()

# Translate the data contract standards into SodaCL
data_contract_parser = DataContractTranslator()
sodacl_yaml_str = data_contract_parser.translate_data_contract_yaml_str(data_contract_yaml_str)

# Log or save the SodaCL checks file to help with debugging  
logging.debug(sodacl_yaml_str)

# Execute the translated SodaCL checks in a scan
scan = Scan()
scan.set_data_source_name("my_business_info")
scan.add_configuration_yaml_file(file_path="configuration.yml")
scan.add_sodacl_yaml_str(sodacl_yaml_str)
scan.execute()
scan.assert_no_checks_fail()