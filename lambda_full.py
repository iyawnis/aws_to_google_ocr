from __future__ import print_function
import json
import urllib
import boto3
import subprocess
import itertools
import urllib2
import time
import traceback
from base64 import b64encode
from io import BufferedReader, StringIO

api_key="API_KEY"
DEST_BUCKET = 'DESTINATION_BUCKET'

API_URL="https://vision.googleapis.com/v1/images:annotate?key=%s" % api_key
TEMP_FILE = '/tmp/tempfile.pdf'
s3 = boto3.client('s3')


def lambda_handler(event, context):
    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))
    extracted_images = []
    extracted_text = []
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        with open(TEMP_FILE, 'w') as temp_file:
            temp_file.write(response['Body'].read())
    except Exception as e:
        traceback.print_exc()
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
    try:
        extracted_images = extract_images_from_pdf()
        print('Retrieved %d images: %s' % (len(extracted_images), extracted_images,))
    except Exception as e:
        traceback.print_exc()
        print(e)
        print('Error extracting images from pdf.')
        raise e
    try:
        extracted_text = request_ocr(extracted_images)
    except Exception as e:
        traceback.print_exc()
        print(e)
        print('Error extracting text from images.')
        raise e
    try:
        upload_extracted_text(extracted_text, key)
    except Exception as e:
        traceback.print_exc()
        print(e)
        print('Error uploading extracted text to S3.')
        raise e
    print('Completed successfully')

def extract_images_from_pdf():
    count = int(subprocess.check_output(['identify', '-format', '%n', TEMP_FILE]))
    print('Number of pdf pages: ', count)
    step = 10 if count > 10 else count
    start = 0
    for range_end in range(step, count + 1, step):
        cmd = "%s[%d-%d]" % (TEMP_FILE, start, range_end,)
        subprocess.check_output(['convert', '-density', '300', cmd,'/tmp/output-%d.jpg'])
        start = range_end + 1
    generated_images = subprocess.check_output(['ls','/tmp'])
    generated_images = filter(lambda x: '.jpg' in x, generated_images.split('\n'))
    return map(lambda x: '/tmp/' + x, generated_images)


def build_request(image_filenames):
    requests = []
    for image in image_filenames:
        if image is not None:
            requests.append({
                "image": {
                    "content": read_image_content(image)
                },
                "features": [
                    {
                        "type": "TEXT_DETECTION"
                    }
                ]
            })
    return requests


def read_image_content(image_file):
    with open(image_file, 'rb') as file:
        return b64encode(file.read()).decode('UTF-8')


def extract_responses_text(responses):
    text_list = []
    for response in responses:
        response_json = json.loads(response)
        for result in response_json['responses']:
            description = result['textAnnotations'][0]['description']
            text_list.append(description.replace('\n', ''))
    return text_list

def grouper(n, iterable, fillvalue=None):
    args = [iter(iterable)] * n
    return itertools.izip_longest(fillvalue=fillvalue, *args)

def request_ocr(image_filenames):
    response_results = []
    grouped_file_names = grouper(5, image_filenames)
    for grouped_images in grouped_file_names:
        request_list = build_request(grouped_images)
        if not request_list:
            continue
        post_data = json.dumps({"requests": request_list}).encode()
        opener = urllib2.build_opener()
        req = urllib2.Request(API_URL, data=post_data,
              headers={'Content-Type': 'application/json'})
        response_results.append(opener.open(req).read())

    return extract_responses_text(response_results)

def upload_extracted_text(extracted_text, key):
    single_string = u"".join(extracted_text)
    reader = StringIO(single_string)
    s3.upload_fileobj(reader, DEST_BUCKET, key.replace('.pdf', '.txt'))