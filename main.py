""" Main python file to perform the required steps """

import os  
import logging as log  
from configparser import RawConfigParser  
from helper import *  

# Gloabl Variable to store config object
config = None

def load_config():
    # Accessing config variable
    global config
    try:
        log.info("loading Config file.")
        # Loading config file
        config = RawConfigParser()
        config.read("config.cfg")
        log.info("Config file loaded successfully.")

    except Exception as e:
        log.error(f"Error in loading config file : {str(e)}.")
        
    return config


def main():
    try:
        log.info("Extracting xml source file url")

        url = config.get("sourcefile", "xml_source_url")

        log.info("Extracting csv file path")
        csv_path = os.path.join(os.getcwd(), config.get("csv", "csv_path"))

        log.info("Extracting xml file's download path")
        
        download_path = os.path.join(
            os.getcwd(), config.get("download", "download_path")
        )

        log.info("Extracting AWS S3 bucket")
        
        bucket_name = config.get("aws", "bucket_name")
        aws_access_key_id = config.get("aws", "aws_access_key_id")
        aws_secret_access_key = config.get("aws", "aws_secret_access_key")
        region_name = config.get("aws", "region_name")

        log.info("Calling download function")
        xml_file = download(url, download_path, "sourcefile.xml")

        
        if not xml_file:
            print("File Download Failed")   
            return

        log.info("Calling parse_source_xml function")
        file_metadata = parse_source_xml(xml_file)

        # checking oif file parsing getting failed or not
        if not file_metadata:
            print("File Parsing Failed")
            return

        filename, file_download_link = file_metadata

        log.info("Calling download function")
        xml_zip_file = download(file_download_link, download_path, filename)

        if not unzip_file(xml_zip_file, download_path):
            print("Extration Failed")
            return

        # Creating absolute path to xml file
        xml_file = os.path.join(download_path, filename.split(".")[0] + ".xml")

        log.info("Calling create csv function")
        csv_file = create_csv(xml_file, csv_path)

        if not csv_file:
            print("XML-CSV Conversion Failed")       
            return

        status = aws_s3_upload(
            csv_file, region_name, aws_access_key_id, aws_secret_access_key, bucket_name
        )
        if not status:
            print("CSV file upload Failed")
            return

        return True

    except Exception as e:
        log.error(f"Error in loading config file : {str(e)}.")


if __name__ == "__main__":
    # Check for the config file loading
    if not load_config():
        print("Error while loading config file")
        exit(1)

    print("Execution started")
    
    if main():
        print("Execution completed successfully")
    else:
        print("Execution Failed.")