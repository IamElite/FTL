import os
import shutil
import zipfile
from urllib import request
from os import path as opath, getenv, rename, remove
from dotenv import load_dotenv
from src.utils.logger import logger

load_dotenv('config.env', override=True)

UPSTREAM_REPO = getenv('UPSTREAM_REPO', "")
UPSTREAM_BRANCH = getenv('UPSTREAM_BRANCH', "main")

if UPSTREAM_REPO:
    config_backup = 'config.env.tmp'
    zip_path = 'update.zip'
    extract_path = 'temp_update'
    
    try:
        # Backup config
        if opath.exists('config.env'):
            rename('config.env', config_backup)
        
        # Build ZIP URL
        # Convert https://github.com/user/repo to https://github.com/user/repo/archive/refs/heads/branch.zip
        clean_repo = UPSTREAM_REPO.rstrip('/')
        if clean_repo.endswith('.git'):
            clean_repo = clean_repo[:-4]
            
        zip_url = f"{clean_repo}/archive/refs/heads/{UPSTREAM_BRANCH}.zip"
        
        logger.info(f"Downloading update from {zip_url}...")
        
        # Download ZIP
        request.urlretrieve(zip_url, zip_path)
        
        # Extract ZIP
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_path)
        
        # The zip usually contains a folder like "repo-branch"
        # We need to find that folder and move its contents
        root_folder = os.listdir(extract_path)[0]
        root_path = os.path.join(extract_path, root_folder)
        
        for item in os.listdir(root_path):
            s = os.path.join(root_path, item)
            d = os.path.join('.', item)
            if os.path.isdir(s):
                if os.path.exists(d):
                    shutil.rmtree(d)
                shutil.move(s, d)
            else:
                if os.path.exists(d):
                    os.remove(d)
                shutil.move(s, d)
        
        logger.info('Successfully updated from UPSTREAM_REPO using ZIP extraction.')
        
    except Exception as e:
        logger.error(f"Something went wrong while updating: {e}")
            
    finally:
        # Restore config
        if opath.exists(config_backup):
            if opath.exists('config.env'):
                remove('config.env')
            rename(config_backup, 'config.env')
            
        # Cleanup
        if opath.exists(zip_path):
            os.remove(zip_path)
        if opath.exists(extract_path):
            shutil.rmtree(extract_path)
else:
    logger.info("UPSTREAM_REPO not found, skipping update.")
