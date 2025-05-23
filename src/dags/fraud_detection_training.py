import logging
import os

import yaml
from dotenv import load_dotenv

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
    level=logging.INFO,
    handlers=[
        logging.FileHandler('./fraud_detection_model.log'),
        logging.StreamHandler()
    ]
)
logger=logging.getLogger(__name__)

class FraudDetectionTraining:
    def __init__(self,config_path='/app/config.yml'):
        os.environ['GIT_PYTHON_REFRESH']='quiet'
        os.environ['GIT_PYTHON_GIT_EXECUTABLE']='usr/bin/git'
        load_dotenv(dotenv_path='/app/.env')
        self.config=self._load_config(config_path)

        os.environ.update({
            'AWS_ACCESS_KEY_ID':os.getenv('AWS_ACCESS_KEY_ID'),
            'AWS_SECRET_ACCESS_KEY':os.getenv('AWS_SECRET_ACCESS_KEY'),
            'AWS_S3_ENDPOINT_URL':self.config['mlflow']['s3_endpoint_url'],
        })

    def _load_config(self,config_path:str)->dict:
        try:
            with open(config_path,'r') as f:
                config=yaml.safe_load(f)
            logger.info('Configuration loaded succesfully')
        except Exception as e:
            logger.info('Configuration failed:%s',str(e))
            raise