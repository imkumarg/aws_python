#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
The python script is responsible for performing the data ingestion task.
Approach:
    1. Get the file name and worksheet name from command line argument.
    2. Download the file from the web url.
    3. Store the file locally.
    4. Read the worksheet name from workbook downloaded.
    5. The worksheet is parsed and list of dictionaries must be created for
       every row.
    6. The dictionary list must be stored in Amazon s3 bucket.
    7. Execution of function must be done using AWS lambda.

Usage:
    python DataIngestion.py <url_file_path> <sheet_name>

Example:
    python DataIngestion.py "https://www.iso20022.org/ISO10383_MIC.xls" "sheet1"
Attributes:

TODO:
    Execution of function must be done using AWS lambda.
"""

__author__ = "Kumar Gaurav"
__date__ = '2018-08-30'
__updated__ = '2018-08-30'
__docformat__ = 'reStructuredText'
__credits__ = ["Kumar Gaurav"]
__version__ = "1.0.0"
__maintainer__ = "Kumar Gaurav"
__status__ = "Released"

# Inbuilt python imports
import os
import sys
import json
import mimetypes

# Return codes
SUCCESS = 0
GENERIC_ERR = 1
IMPORT_ERR = 2
ATTRIBUTE_ERR = 3
INVALID_ARG_ERR = 4
S3_BUCKET_CR_ERR = 5

# Global variables
FILE_URL_PATH = None
SHEET_NAME = None

# Content type constants
CONTENT_TYPE_TEXT = 'text'
CONTENT_TYPE_HTML = 'html'

# Local file name paths
SCRIPT_BASE_PATH = os.path.dirname(os.path.realpath(__file__))
OUTPUT_FILE_PATH = os.path.join(SCRIPT_BASE_PATH, "output",
                                "dataingestion")
OUTPUT_FILE_JSON = os.path.join(SCRIPT_BASE_PATH, "output",
                                "dataingestion.json")
JSON_FILE_PATH_ON_AWS = 'DataIngestion/' + os.path.basename(OUTPUT_FILE_JSON)

# Required python imports
try:
    import requests
    import pandas as pd
    import boto3
except ImportError as ie:
    print("Import Error!! Unable to import module(s)."
          "\nInstall the module and try again..."
          "\nException: ", ie)
    sys.exit(IMPORT_ERR)


class DataIngestion(object):
    """
    The class is responsible to perform the data ingestion task. It will
    download the file from url parse it, convert it in json and store in
    Amazon s3 bucket.

    Attributes:
        __aws_bucket_name (str): Holds the aws_bucket_name
        __response (requests): Holds the object type of requests connection.
        __url_file_path (str): Holds the url path of the file to be downloaded.
        __sheet_name (str): Holds the sheet name of the workbook file which
                            needs to be parsed.

    Raises:
        AttributeError: It raises attribute error exception when the attribute
                        is not present in the class.
    """

    def __init__(self, aws_bucket_name, url_file_path_argv, sheet_name_argv):
        """Object initializer to initialize the class members

        Args:
            aws_bucket_name: AWS bucket name coming from command line argument
            url_file_path_argv: File path coming from command line
                                argument
            sheet_name_argv: Sheet name of the file coming from the
                             command line argument
        """
        self.__response = None
        self.__aws_bucket_name = aws_bucket_name
        self.__url_file_path = url_file_path_argv
        self.__sheet_name = sheet_name_argv

    def __getattr__(self, item):
        """Prints error message if attribute is not found"""
        print("Class attribute not found: ", item)
        return ATTRIBUTE_ERR

    def __getattribute__(self, item):
        """Checks for every attribute"""
        if not item:
            raise AttributeError

        return object.__getattribute__(self, item)

    def __repr__(self):
        """To print the printable version of the object"""
        return "DataIngestion(\nResponse=%s," \
               "\nURL File Path=%s," \
               "\nSheet name=%s" \
               "\n)" %\
               (self.__response,
                self.__url_file_path,
                self.__sheet_name)

    @property
    def prop_response(self):
        """requests: Property to get the object of requests"""
        return self.__response

    @prop_response.setter
    def prop_response(self, value):
        """Property to set the request obj

        Args:
             value (requests): response is passed as argument
        """
        self.__response = value

    @prop_response.deleter
    def prop_response(self):
        """Property to delete the requests object"""
        del self.__response

    @property
    def prop_aws_bucket_name(self):
        """requests: Property to get the aws bucket name"""
        return self.__aws_bucket_name

    @prop_aws_bucket_name.setter
    def prop_aws_bucket_name(self, value):
        """Property to set the aws bucket name

        Args:
             value (requests): response is passed as argument
        """
        self.__aws_bucket_name = value

    @prop_aws_bucket_name.deleter
    def prop_aws_bucket_name(self):
        """Property to delete the aws bucket name"""
        del self.__aws_bucket_name

    @property
    def prop_url_file_path(self):
        """str: Property to get the url file path"""
        return self.__url_file_path

    @prop_url_file_path.setter
    def prop_url_file_path(self, value):
        """Property to set the url file path

        Args:
             value (str): Url file path passed as an argument
        """
        self.__url_file_path = value

    @prop_url_file_path.deleter
    def prop_url_file_path(self):
        """Property to delete the url file path"""
        del self.__url_file_path

    @property
    def prop_sheet_name(self):
        """str: Property to get the sheet name"""
        return self.__sheet_name

    @prop_sheet_name.setter
    def prop_sheet_name(self, value):
        """Property to set the url file path

        Args:
            value (str): Sheet name to be passed as an argument.
        """
        self.__sheet_name = value

    @prop_sheet_name.deleter
    def prop_sheet_name(self):
        """Property to delete the sheet name"""
        del self.__sheet_name

    @staticmethod
    def help():
        """Function to display help message on failures"""
        print("\nUsage:"
              "\npython DataIngestion.py <aws_s3_bucket_name> "
              "<url_input_file> <sheet_name>"
              "\nExample:"
              "\npython DataIngestion.py 'Your-AWS-S3-Bucket-Name' "
              "'https://www.iso20022.org/sites/default/files/ISO10383_MIC/"
              "ISO10383_MIC.xls' 'MICs List by CC'")

    def get_cmd_argv(self):
        """Function to take the input from the command line argument and store
        that in class members
        """
        ret_status = False
        try:
            if len(sys.argv) == 4:
                self.__aws_bucket_name = sys.argv[1].strip("'").strip()
                self.__url_file_path = sys.argv[2].strip("'").strip()
                self.__sheet_name = sys.argv[3].strip("'").strip()
                print("\nArguments: ")
                print("Info: AWS S3 Bucket Name: ", self.__aws_bucket_name)
                print("Info: URL Path: ", self.__url_file_path)
                print("Info: Sheet name to parse: ", self.__sheet_name)
                if self.__aws_bucket_name and self.__url_file_path and \
                        self.__sheet_name:
                    ret_status = True
                else:
                    print("\nInvalid Argument!!")
                    DataIngestion.help()
                    sys.exit(INVALID_ARG_ERR)
            else:
                print("\nInvalid Argument!!")
                DataIngestion.help()
                sys.exit(INVALID_ARG_ERR)

        except Exception as e:
            print("Error: In getting the input from command line."
                  "\nException: ", e)

        return ret_status

    def connect(self):
        """
        Function to send the requests to url and get the response.
        The response is stored in the response object

        Return:
            bool
        """
        ret_status = False
        try:
            print("\nPerforming data ingestion task."
                  " Waiting for the response...")
            self.__response = requests.get(self.__url_file_path)
            if self.__response is not None:
                print("Info: Successfully received the response for the url "
                      "path. [Status code:", self.__response.status_code, "]")
                ret_status = True
            else:
                print("Info: Unable to get the response for the url path"
                      "\nStatus code: ", self.__response.status_code)

        except Exception as e:
            print("Error: Establishing connection to the url: ",
                  self.__url_file_path, "\nException: ", e)

        return ret_status

    def is_downloadable(self):
        """
        Function to check whether the url file path is downloadable or not

        Return:
             bool
        """
        ret_status = True
        try:
            # Getting the content-type from the response object
            content_type = self.__response.headers.get('Content-Type', None)

            if content_type is None:
                ret_status = False
            else:
                if CONTENT_TYPE_TEXT in content_type.lower():
                    ret_status = False

                if CONTENT_TYPE_HTML in content_type.lower():
                    ret_status = False

        except Exception as e:
            print("Error: Unable to get the downloadable status of url: ",
                  self.__url_file_path, "\nException: ", e)

        return ret_status

    def get_extensions(self, strict_flag=False, all_ext_flag=False):
        """
        Function to get the extension(s) of the requested url.

        Args:
            strict_flag (bool): The strict flag specifies the list of known MIME
                                types registered with IANA. If False is given
                                some additional non-standard commonly used MIME
                                types are also considered.
            all_ext_flag (bool): Flag to ensure to get all the extensions for
                                 the requested url path
        """
        extension_list = list()
        try:
            # Getting the content-type from the response object
            content_type = self.__response.headers.get('Content-Type', None)

            if content_type is not None:
                if all_ext_flag:
                    extension_list = mimetypes.guess_all_extensions(
                        content_type,
                        strict_flag)
                else:
                    extension_list = mimetypes.guess_extension(
                        content_type,
                        strict_flag).split()

        except Exception as e:
            print("Error: In getting the extension(s) of url path: ",
                  self.__url_file_path, "\nException: ", e)

        return extension_list

    def download(self):
        """
        Function to download the file from the url.

        Return:
            bool
        """
        ret_status = False
        try:
            if self.__response.status_code == requests.codes.ok:
                # Writing the file to local
                if os.path.isfile(OUTPUT_FILE_PATH):
                    os.remove(OUTPUT_FILE_PATH)
                    print("Info: Deleted the old file: ", OUTPUT_FILE_PATH)

                print("Info: Downloading the url file path. Please wait...")
                with open(OUTPUT_FILE_PATH, "wb") as file_object:
                    file_object.write(self.__response.content)

                if os.path.isfile(OUTPUT_FILE_PATH):
                    print("Info: Created output file successfully. "
                          "Path:", OUTPUT_FILE_PATH)

                    # Getting size of the file
                    file_size = os.path.getsize(OUTPUT_FILE_PATH)

                    if file_size > 0:
                        print("Info: Contents written in the file"
                              " successfully.")
                        ret_status = True

                    else:
                        print("Info: Output file empty/Unable to write "
                              "contents in it.")
                else:
                    print("Info: Unable to create new output file."
                          "\nPath: ", OUTPUT_FILE_PATH)

        except Exception as e:
            print("Error: Downloading the file from the url path: ",
                  self.__url_file_path, "\nException: ", e)

        return ret_status

    def parse_file(self):
        """
        Function to parse the downloaded file. Create the records of dictionary
        and dump that records as json format

        Return:
            bool
        """
        ret_status = False
        try:
            # Reading the excel file using pandas
            pd_xls_obj = pd.ExcelFile(OUTPUT_FILE_PATH)

            # Checking for the sheet name in the downloaded excel file
            if self.__sheet_name.strip() in pd_xls_obj.sheet_names:
                print("Info: Sheet name -", self.__sheet_name,
                      "found in the file.")

                # Read the excel sheet
                xls_data_df = pd.read_excel(OUTPUT_FILE_PATH, self.__sheet_name)

                if not xls_data_df.empty:
                    print("Info: Successfully converted the excel content to "
                          "data frame")

                    # Replacing the NaN in data frame with "None"
                    xls_data_df = xls_data_df.where(pd.notnull(xls_data_df),
                                                    None)

                    # Creating a list of dictionary with all the rows with first
                    # row as key in each dictionary
                    dict_list_records = xls_data_df.to_dict('records')

                    if dict_list_records:
                        print("Info: Successfully converted the records to "
                              "dictionary list")

                        # Writing the dumped json object to file
                        with open(OUTPUT_FILE_JSON, "w") as file_object:
                            # Dumping the records in JSON format
                            json.dump(dict_list_records, file_object)

                        if os.path.isfile(OUTPUT_FILE_JSON):
                            print("Info: Created JSON file successfully. "
                                  "Path:", OUTPUT_FILE_JSON)
                            if os.path.getsize(OUTPUT_FILE_JSON) > 0:
                                print("Info: JSON content written "
                                      "successfully.")
                                ret_status = True
                            else:
                                print("Info: Unable to write the JSON content.")
                    else:
                        print("Info: Unable to converts the records to "
                              "dictionary list")
                else:
                    print("Info: Unable to read the excel file and convert to "
                          "data frame")
            else:
                print("Info: Sheet name -", self.__sheet_name,
                      "not found in file.")
        except Exception as e:
            print("Error: Parsing the excel file.\nException: ", e)

        return ret_status

    def upload_to_aws_s3(self):
        """
        Function to connect aws s3 bucket and upload the created json file

        Return:
            bool
        """
        ret_status = False
        cr_bucket_status = None
        try:
            print("Info: Uploading file to AWS S3 bucket. Please wait...")
            # Checking current s3 bucket existence in aws
            # Creating the object of s3
            s3_obj = boto3.resource("s3")
            if not self.__aws_bucket_name.lower() in [each_bucket.name.lower()
                                                      for each_bucket in
                                                      s3_obj.buckets.all()]:
                print("Info: AWS S3 bucket:", self.__aws_bucket_name.lower(),
                      "not found.")

                print("Info: Creating the AWS S3 bucket...")
                cr_bucket_status = s3_obj.create_bucket(
                    Bucket=self.__aws_bucket_name.lower(),
                    CreateBucketConfiguration={
                        'LocationConstraint': 'ap-south-1'
                    }
                )

                if cr_bucket_status is None:
                    print("Info: Failed to create AWS S3 bucket:",
                          self.__aws_bucket_name.lower())
                    sys.exit(S3_BUCKET_CR_ERR)

            # Uploading the file to bucket
            json_buf = open(OUTPUT_FILE_JSON, 'rb')
            response = s3_obj.Bucket(self.__aws_bucket_name.lower()).put_object(
                ACL='public-read',
                Key=JSON_FILE_PATH_ON_AWS,
                Body=json_buf
            )
            if response:
                print("Info: File:", OUTPUT_FILE_JSON,
                      "uploaded successfully to AWS S3 bucket:",
                      self.__aws_bucket_name.lower())
                ret_status = True

        except Exception as e:
            print("Error: Uploading the file to AWS s3 bucket:",
                  self.__aws_bucket_name.lower(), "\nException:", e)

        return ret_status

    def main(self):
        """
        Main function for the class to trigger the process and execute the
        required task
        """
        ret_status = GENERIC_ERR
        global OUTPUT_FILE_PATH
        try:
            # Invoking the function to get the command line argument input
            status_argv = self.get_cmd_argv()
            if status_argv:
                # Creating output directory if not created
                if not os.path.exists(os.path.dirname(OUTPUT_FILE_PATH)):
                    os.makedirs(os.path.dirname(OUTPUT_FILE_PATH))

                # Invoking function to request to the url path and get response
                status_connect = self.connect()
                if status_connect:
                    # Invoking function to check if url is downloadable or not
                    status_downloadable = self.is_downloadable()
                    if status_downloadable:
                        # By default getting the extension from the path
                        file_ext = self.__url_file_path.split('.')[-1] if len(
                            self.__url_file_path.split('.')) > 0 else ""

                        file_ext = "." + file_ext.strip()

                        # Invoking function to get the extension list
                        ext_list = self.get_extensions(False, True)
                        if ext_list and file_ext in ext_list:
                            OUTPUT_FILE_PATH = OUTPUT_FILE_PATH + file_ext
                            print("Info: Output file path: ", OUTPUT_FILE_PATH)
                            # Invoking function to download the file
                            status_download = self.download()
                            if status_download:
                                status_parse = self.parse_file()

                                if status_parse:
                                    status_upload = self.upload_to_aws_s3()
                                    if status_upload:
                                        ret_status = SUCCESS
                        else:
                            print("Info: Unable to get the extensions of the "
                                  "url file path")
                    else:
                        print("Info: Given url: %s path is not downloadable." %
                              self.__url_file_path)
                else:
                    print("Info: Unable to get response for the url: ",
                          self.__url_file_path)
            else:
                print("Error: In getting input from command line argument.")

        except Exception as e:
            print("Error: Performing the data ingestion process..."
                  "\nException: ", e)

        return ret_status


# Entry section
if __name__ == '__main__':
    # Creating the object of DataIngestion class and invoking main() function
    di_object = DataIngestion(None, None, None)
    status_main = di_object.main()
    sys.exit(status_main)
