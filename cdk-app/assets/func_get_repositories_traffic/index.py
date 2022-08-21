import boto3
import os
import requests

from http import HTTPStatus
from botocore.exceptions import ClientError
from requests.exceptions import RequestException


# Modify this list to include the repositories you want to track (you must have push access)
REPOS = [
    {
        'name': 'github-traffic-capture',
        'owner': 'bpguasch'
    }
]

GITHUB_API_URL_FORMAT = 'https://api.github.com/repos/{}/{}/traffic/{}'
GITHUB_TRAFFIC_CLONES_ENDPOINT = 'clones'
GITHUB_TRAFFIC_VIEWS_ENDPOINT = 'views'


class RequestError(Exception):
    def __init__(self, status: int, error_info: str):
        self.__error_info = error_info
        self.__status = status

    @property
    def status(self) -> int:
        return self.__status

    @property
    def error_info(self) -> str:
        return self.__error_info


def resolve_secret(secret_id: str, region_name: str) -> str:
    session = boto3.session.Session()

    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    print('Getting GitHub access token...')

    try:
        return client.get_secret_value(
            SecretId=secret_id
        )['SecretString']
    except ClientError as error:
        raise RequestError(
            status=HTTPStatus.INTERNAL_SERVER_ERROR,
            error_info=error.response['Error']
        )


def init_traffic_data(repo_name: str, timestamp: str) -> dict:
    return {
        'views-count': 0,
        'views-uniques': 0,
        'clones-count': 0,
        'clones-uniques': 0,
        'repo-name': repo_name,
        'timestamp': timestamp
    }


def get_repo_traffic(token: str, repo: dict) -> [dict]:
    url = GITHUB_API_URL_FORMAT.format(repo['owner'], repo['name'], '{}')
    params = {'per': 'day'}
    headers = {'Authorization': 'token {}'.format(token)}
    traffic = {}

    print('Getting traffic for repo {}...'.format(repo['name']))

    # Request views and clones
    for endpoint in [GITHUB_TRAFFIC_VIEWS_ENDPOINT, GITHUB_TRAFFIC_CLONES_ENDPOINT]:
        try:
            response = requests.get(url.format(endpoint), params=params, headers=headers)
            status = response.status_code
            data = response.json()

            if status != HTTPStatus.OK:
                raise RequestError(status=status, error_info=data)
            else:
                # For every retrieved day...
                for day in data[endpoint]:
                    if day['timestamp'] not in traffic:
                        traffic[day['timestamp']] = init_traffic_data(repo['name'], day['timestamp'])

                    traffic[day['timestamp']]['{}-{}'.format(endpoint, 'count')] = day['count']
                    traffic[day['timestamp']]['{}-{}'.format(endpoint, 'uniques')] = day['uniques']
        except RequestException as e:
            raise RequestError(status=HTTPStatus.INTERNAL_SERVER_ERROR, error_info=str(e))

    return [value for value in traffic.values()]


def put_traffic_data(table_name: str, traffic: [dict]) -> None:
    client = boto3.resource('dynamodb')
    table = client.Table(table_name)

    with table.batch_writer() as batch:
        for day in traffic:
            batch.put_item(Item=day)


def handler(event, context):
    secret_id = os.environ['SECRET_ARN']
    table_name = os.environ['TABLE_NAME']
    region_name = os.environ['AWS_REGION']

    try:
        # Get the GitHub access token from Secrets Manager
        github_access_token = resolve_secret(secret_id, region_name)
    except RequestError as error:
        print(error.error_info)

        return {
            'statusCode': error.status,
            'body': error.error_info
        }
    else:
        # For every repository...
        for repo in REPOS:
            try:
                # Get traffic data
                traffic = get_repo_traffic(github_access_token, repo)

                # Put traffic data in DynamoDB
                put_traffic_data(table_name, traffic)
            except RequestError as error:
                print(error.error_info)

        return {
            'statusCode': 200
        }
