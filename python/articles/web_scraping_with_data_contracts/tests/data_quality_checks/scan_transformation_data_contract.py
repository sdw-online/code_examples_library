from soda.contracts.data_contract_translator import DataContractTranslator
from soda.scan import Scan
import logging
import os

def run_dq_checks_for_transformation_stage():
    # Correctly set the path to the project root directory
    project_root_directory = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    # Construct the full file paths for the YAML files
    transformation_config_yaml_path = os.path.join(project_root_directory, 'config', 'transformation_config.yml')
    transformation_data_contract_path = os.path.join(project_root_directory, 'tests', 'data_contracts', 'transformation_data_contract.yml')

    # Read the data contract file as a Python string
    with open(transformation_data_contract_path) as f:
        data_contract_yaml_str: str = f.read()

    # Translate the data contract standards into SodaCL
    data_contract_parser = DataContractTranslator()
    sodacl_yaml_str = data_contract_parser.translate_data_contract_yaml_str(data_contract_yaml_str)

    # Log or save the SodaCL checks file to help with debugging
    logging.debug(sodacl_yaml_str)

    # Execute the translated SodaCL checks in a scan
    scan = Scan()
    scan.set_data_source_name("transformed_fb_data")
    scan.add_configuration_yaml_file(file_path=transformation_config_yaml_path)
    scan.add_sodacl_yaml_str(sodacl_yaml_str)
    scan.execute()
    scan.assert_no_checks_fail()

if __name__ == "__main__":
    run_dq_checks_for_transformation_stage()
