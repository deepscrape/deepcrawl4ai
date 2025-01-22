import os
from dotenv import load_dotenv
import firebase_admin.auth
import firebase_admin.db
import firebase_admin.firestore
import firebase_admin.storage
import firebase_admin
from firebase_admin import credentials
import json

# Load environment variables
load_dotenv()

firestoreConfig = json.loads(os.getenv("FIRESTORE_CONFIG"))
firebase_credentials = json.loads(os.getenv("FIREBASE_CREDENTIALS"))

# Load Firebase credentials
company_credentials = credentials.Certificate(firebase_credentials)

# Initialize Firebase App
firebase_admin.initialize_app(company_credentials)


# Set up Firestore with a custom database ID
dbName = firestoreConfig["dbName"]  # Replace with your Firestore database ID
db = firebase_admin.firestore.client(database_id=dbName)

# Assign the authentication object
auth = firebase_admin.auth

print(
    f"\033[94mINFO:\033[0m     \033[92mFirestore initialized by\033[0m \033[95m{dbName}\033[0m \033[94mdatabase\033[0m"
)
