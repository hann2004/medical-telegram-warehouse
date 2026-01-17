import os
import csv
from ultralytics import YOLO
from tqdm import tqdm

# Paths
IMAGE_ROOT = "data/raw/images"
RESULTS_CSV = "data/yolo_detections.csv"
MODEL_PATH = "yolov8n.pt"

# Classes for YOLOv8n
PERSON_CLASS = 0  # COCO class index for 'person'
BOTTLE_CLASS = 39  # COCO class index for 'bottle'
CONTAINER_CLASSES = [39, 41, 46, 47, 48, 49, 50, 51, 52, 53, 54]  # bottle, cup, bowl, wine glass, etc.

# Classification scheme
CATEGORY_MAP = {
    "promotional": lambda classes: PERSON_CLASS in classes and any(c in classes for c in CONTAINER_CLASSES),
    "product_display": lambda classes: any(c in classes for c in CONTAINER_CLASSES) and PERSON_CLASS not in classes,
    "lifestyle": lambda classes: PERSON_CLASS in classes and not any(c in classes for c in CONTAINER_CLASSES),
    "other": lambda classes: PERSON_CLASS not in classes and not any(c in classes for c in CONTAINER_CLASSES),
}

def classify(classes):
    for cat, rule in CATEGORY_MAP.items():
        if rule(classes):
            return cat
    return "other"

def scan_images():
    image_files = []
    for root, _, files in os.walk(IMAGE_ROOT):
        for f in files:
            if f.lower().endswith((".jpg", ".jpeg", ".png")):
                image_files.append(os.path.join(root, f))
    return image_files

def main():
    model = YOLO(MODEL_PATH)
    image_files = scan_images()
    results = []
    for img_path in tqdm(image_files, desc="YOLO Detection"):
        try:
            pred = model(img_path)
            boxes = pred[0].boxes
            detected_classes = set()
            for box in boxes:
                detected_classes.add(int(box.cls[0].item()))
            category = classify(detected_classes)
            for box in boxes:
                results.append({
                    "image_path": img_path,
                    "detected_class": int(box.cls[0].item()),
                    "confidence_score": float(box.conf[0].item()),
                    "image_category": category
                })
        except Exception as e:
            print(f"Error processing {img_path}: {e}")
    # Save to CSV
    with open(RESULTS_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["image_path", "detected_class", "confidence_score", "image_category"])
        writer.writeheader()
        writer.writerows(results)
    print(f"Detection results saved to {RESULTS_CSV}")

if __name__ == "__main__":
    main()
