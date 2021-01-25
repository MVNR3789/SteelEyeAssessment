"""
This module takes an input xml and reads through it to get the zip file link.

Once it gets the zip file url it downloads the same and extracts another xml present in it
Then it parses through the xml and
Converts the contents of the xml into a CSV with the following header:
FinInstrmGnlAttrbts.Id
FinInstrmGnlAttrbts.FullNm
FinInstrmGnlAttrbts.ClssfctnTp
FinInstrmGnlAttrbts.CmmdtyDerivInd
FinInstrmGnlAttrbts.NtnlCcy
Issr
"""


import os
import sys
import boto3
import logging
import zipfile
import requests

import xml.etree.ElementTree as ET

logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger(__name__)


def get_download_link(input_xml):
    """
    This function reads the response.xml and gets the download_link specified
    """

    LOG.info(f"Getting the download_link from {input_xml}")

    download_link = ""
    flag = 0
    for event, elem in ET.iterparse(input_xml, events=('start', 'end')):
        if event=='start' and elem.tag == 'doc':
            flag = 1
            continue
        if flag:
            if elem.attrib['name']=='download_link':
                 download_link = elem.text.strip()
                 break
    
    LOG.info(f"download_link for zipfile is: '{download_link}'\n")
    # print("download_link:", download_link)
    if download_link=='':
        LOG.error(f"Couldn't find the download_link from {input_xml}")

    return download_link


def download_and_extract_xml(zip_url, extract_directory, chunk_size=128):
    """
    This function downloads the zip file from the given zip_url
    and returns the xml file extracted from the same
    """

    LOG.info(f"Downloading the zip file from {zip_url}")

    zip_file_name = zip_url.split('/')[-1]

    try:
        r = requests.get(zip_url, stream=True)
        with open(zip_file_name, 'wb') as fd:
            for chunk in r.iter_content(chunk_size=chunk_size):
                fd.write(chunk)
    except Exception as e:
        LOG.error("Couldn't download the zipfile..", exc_info=True)
        sys.exit()
    

    LOG.info(f"Downloaded the zip file: {zip_file_name}\n")

    LOG.info(f"Extracting from...: {zip_file_name}")

    zip = zipfile.ZipFile(zip_file_name)
    file_members = zip.infolist()
    max_file_member = None

    for file_member in file_members:
        if (not max_file_member or file_member.file_size > max_file_member.file_size):
            max_file_member = file_member

    zip.extractall(path=extract_directory)
    zip.close()

    LOG.info(f"Extracted file is...: {max_file_member.filename}\n")

    return extract_directory + os.sep + max_file_member.filename


def generate_csv_from_xml(xml, name_space, attribute, tag_list):
    """
    This iterates over the xml and read the required tags under the given attribute
    and finally generates the csvs with all the required information
    """

    results = []

    flag = 0
    rmap = {}

    LOG.info(f"Parsing xml...: {xml}")

    # i=0
    for event, elem in ET.iterparse(xml, events=('start', 'end')):
        # i+=1; if i==500:break
        # print (event, elem.tag, elem.text)
        # if event=='end' and elem.tag == name_space+'Issr':
        #     rmap['Issr'] = elem.text.strip()
        #     continue

        if event=='start' and elem.tag == name_space+attribute:
            flag = 1
            continue

        if flag==1:
            for column in tag_list:
                if event=='end' and elem.tag == name_space+column:
                    rmap[column] = elem.text.strip()
                    break

        if event=='end' and elem.tag == name_space+'Issr':
            rmap['Issr'] = elem.text.strip()
            # print ("rmap:", rmap)
            results.append(rmap.copy())
            rmap.clear()
            flag = 0

    LOG.info(f"Total records retreived:{len(results)}\n")

    LOG.info("Generating CSV from the pasrsed ressult...")

    output_csv = 'results.csv'
    with open(output_csv, 'w') as wf:
        wf.write(",".join(tag_list)+"\n")
        for result in results:
            line = ",".join([result[tag] for tag in tag_list])
            wf.write(line+"\n")

    LOG.info(f"Generated {output_csv}\n\n")

    return output_csv


def upload_to_s3(filename):
    "This is not fully tested.. But should work with little bit changes..."

    LOG.info(f"Uploading {filename} to AWS::S3")
    try:
        s3 = boto3.resource('s3')

        bucket = 'bucket_name'
        s3.meta.client.upload_file(Filename=filename, Bucket=bucket, Key=filename)
    except Exception as e:
        LOG.error("Issue while uploading csv to s3", exc_info=True)



if __name__ == '__main__':

    input_xml = 'response.xml'
    name_space = '{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}'
    tag_list = ['Id', 'FullNm', 'ClssfctnTp', 'ClssfctnTp', 'CmmdtyDerivInd', 'NtnlCcy', 'Issr']
    attribute = 'FinInstrmGnlAttrbts'


    download_link = get_download_link(input_xml)
    xml_generated = download_and_extract_xml(download_link, extract_directory='.')
    output_csv = generate_csv_from_xml(xml_generated, name_space, attribute, tag_list)
    
    # This is not tested
    # upload_to_s3('output_csv')


