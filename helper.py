""" Module containing all the helper functions for the process """

import os  
import boto3  
import zipfile  
import requests  
import pandas as pd  
import logging as log  
from xml.etree import ElementTree as ET  


def download(url, download_path, filename):
    file = ""
    log.info("Downloading the xml file.")
    
    try:
        response = requests.get(url)

        file_ext = ["xml", "html"]
        if (filename.split(".")[-1] in file_ext):
            if(filename.split(".")[-1] not in response.text):
                return file

        # Checking if the requests got a correct response
        if response.ok:
            # Creating the filepath for downloading xml file
            os.makedirs(download_path)
            file = os.path.join(download_path, filename)

            # Creating the xml file at the path with the given file name
            with open(file, "wb") as f:
                f.write(response.content)
                log.info(f"xml file downloaded at {download_path}")

        else:
            # Logging if the download of the xml file fails
            log.error("Error while downloading the xml file")

    except Exception as e:
        # Logging if the download of the xml file fails
        log.error(f"Error occurred - {str(e)}")

    return file


def parse_source_xml(xml_file):
    """Parses the source xml file
    Param(s):
        xml_file (str)        :   Path to the xml file
    Return(s):
        download_link (str)   :   Link to download target xml file
    """
    try:
        log.info("Loading the xml file.")
        # Loading the xml file content
        xmlparse = ET.parse(xml_file)

        log.info("Parsing the xml file.")
        root = xmlparse.getroot()[1]
        docs = root.findall("doc")

        # Traversing through all the doc tag elements
        for doc in docs:

            log.info("Extracting the file type")
            # Extracting file type of the doc
            file_type = doc.find(".//str[@name='file_type']")

            # Checking if the file type of the doc 'DLTINS'
            if file_type.text == "DLTINS":

                log.info("Match found for file type DLTINS")

                log.info("Extracting the file name")
                file_name = doc.find(".//str[@name='file_name']").text

                log.info("Extracting the file download link")
                download_link = doc.find(".//str[@name='download_link']").text
                break
        else:
            log.info("Match not found for file type DLTINS")
            return

        return file_name, download_link

    except Exception as e:
        log.error(f"Error occurred - {str(e)}")


def unzip_file(zipped_file, uncompressed_file_path):
    """Unzips the compressed file to the provided path
    Param(s):
        zipped_file (str)               : Compressed File path
        uncompressed_file_path (str)    : Path to store the uncompressed file
    """
    try:        
        with zipfile.ZipFile(zipped_file, "r") as zip_ref:
            zip_ref.extractall(uncompressed_file_path)

        log.info("Compressed file Extracted")
        return True

    except Exception as e:
        log.error(f"Error occurred while extracting - {str(e)}")
        return False


def create_csv(xml_file, csv_path):
    try:
        
        # Creating the path
        log.info("Creating CSV file path")
        os.makedirs(csv_path)

        # Extracting the csv file name from xml file
        csv_filename = xml_file.split(os.sep)[-1].split(".")[0] + ".csv"

        # Creating csv file path
        csv_file = os.path.join(csv_path, csv_filename)

        log.info("Loading the xml file")
        # Creating xml file itertor
        xml_iter = ET.iterparse(xml_file, events=("start",))

        csv_columns = [
            "FinInstrmGnlAttrbts.Id",
            "FinInstrmGnlAttrbts.FullNm",
            "FinInstrmGnlAttrbts.ClssfctnTp",
            "FinInstrmGnlAttrbts.CmmdtyDerivInd",
            "FinInstrmGnlAttrbts.NtnlCcy",
            "Issr",
        ]

        # Creating empty dataframe with the required column names
        df = pd.DataFrame(columns=csv_columns)

        # List to store the extr4acted data
        extracted_data = []

        log.info("Parsing the xml file and extracting required dataa")

        
        # Traversing the xml data
        for event, element in xml_iter:
            if event == "start":
                if "TermntdRcrd" in element.tag:
                    data = {}

                    reqd_elements = [
                        (elem.tag, elem)
                        for elem in element
                        if "FinInstrmGnlAttrbts" in elem.tag or "Issr" in elem.tag
                    ]

                    # Traversing through the required tags
                    for tag, elem in reqd_elements:

                        if "FinInstrmGnlAttrbts" in tag:
                            for child in elem:
                                if "Id" in child.tag:
                                    data[csv_columns[0]] = child.text
                                elif "FullNm" in child.tag:
                                    data[csv_columns[1]] = child.text
                                elif "ClssfctnTp" in child.tag:
                                    data[csv_columns[2]] = child.text
                                elif "CmmdtyDerivInd" in child.tag:
                                    data[csv_columns[3]] = child.text
                                elif "NtnlCcy" in child.tag:
                                    data[csv_columns[4]] = child.text

                        else:
                            data[csv_columns[5]] = child.text

                    # Appending the single element extracted data in the list
                    extracted_data.append(data)

        log.info("All the required data extracted from xml file")

        # Appending the extracted data in the data frame
        df = df.append(extracted_data, ignore_index=True)

        log.info("Dropping empty rows")
        # Removes empty rows from the dataframe
        df.dropna(inplace=True)

        log.info("Creating the CSV file")
        # Creates csv file from the dataframe
        df.to_csv(csv_file, index=False)

        # returning the csv file path
        return csv_file

    except Exception as e:
        log.error(f"Error occurred while extracting - {str(e)}")


def aws_s3_upload(
    file, region_name, aws_access_key_id, aws_secret_access_key, bucket_name
):
    try:
        # Extracting the file name from the path
        filename_in_s3 = file.split(os.sep)[-1]

        log.info("Creating S3 resource object")
        # Connecting to S3 bucket with boto3
        s3 = boto3.resource(
            service_name="s3",
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

        log.info("Uploading the file to s3 bucket")
        # Uploads the file to the s3 bucket
        s3.Bucket(bucket_name).upload_file(Filename=file, Key=filename_in_s3)

        log.info("File uploaded successfully to s3 bucket")
        return True

    except Exception as e:
        log.error(f"Error occurred while extracting - {str(e)}")