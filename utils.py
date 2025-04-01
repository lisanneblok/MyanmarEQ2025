import yaml
from pathlib import Path
from tqdm.auto import tqdm

# image processing
import os
import numpy as np
import cv2
import pytesseract
from PIL import Image


def read_yaml(fp: str | Path) -> dict:
    with open(fp, 'r') as f:
        config = yaml.safe_load(f)
        
    return config
        
        
def specify_utc(dt):
    """
    Specify UTC timezone for a datetime object
    """
    import pytz
    from datetime import datetime

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=pytz.UTC)
    else:
        dt = dt.astimezone(pytz.UTC)
        
    return dt

def append_to_csv(fp: str | Path, data: list[dict], mode='a'):
    """
    Append data to a CSV file
    """
    import pandas as pd

    df = pd.DataFrame(data)
    
    if mode == 'a':
        df.to_csv(fp, mode=mode, header=False, index=False)
    else:
        df.to_csv(fp, mode=mode, header=True, index=False)
        
        
def binarize_image(image_path):
    # Load image
    img = cv2.imread(image_path)
    
    # Convert to grayscale
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    
    # Apply threshold to get binary image
    # _, binary = cv2.threshold(gray, 150, 255, cv2.THRESH_BINARY_INV)
    blur = cv2.GaussianBlur(gray, (3,3), 0)
    binary = cv2.threshold(blur, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)[1]
    return binary

def image_contains_text(image_path, confidence_threshold=10):
    try:
        # Binarize image
        binary = binarize_image(image_path)
        
        # Use pytesseract to detect text
        text_data = pytesseract.image_to_data(binary, output_type=pytesseract.Output.DICT)
        
        has_text = False
        # aggregate confidence values
        if sum(conf > 90 for conf in text_data['conf']) <= confidence_threshold:
            has_text = True
        return has_text
    
    except Exception as e:
        print(f"Error processing {image_path}: {e}")
        return False


def filter_images_without_text(directory):
    non_text_images = []
    text_images = []
    
    for filename in tqdm(Path(directory).iterdir(), desc="Scanning images for overt amounts of text"):
        if filename.suffix.lower() in ('.jpg', '.jpeg'):
            if image_contains_text(filename):
                text_images.append(filename.name)
            else:
                non_text_images.append(filename.name)
    
    return non_text_images, text_images