import os
import signal
import sys
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
# config = load_config()

fire_config = os.getenv("FIRESTORE_CONFIG")
firebase_cred = os.getenv("FIREBASE_CREDENTIALS")



if not fire_config or not firebase_cred:
    raise ValueError("FIRESTORE_CONFIG and FIREBASE_CREDENTIALS environment variables must be set")


firestoreConfig = json.loads(fire_config)
firebase_credentials = json.loads(firebase_cred)


class FirebaseClient:
    def __init__(self):
        self.db = None
        self.auth = None
        self.firestoreConfig = firestoreConfig
        self.firebase_credentials = firebase_credentials
        self.app = None  # Store the Firebase app instance
        # Load Firebase credentials
        self.company_credentials = credentials.Certificate(self.firebase_credentials)

    def init_firebase(self):
        # # Initialize Firebase if not already initialized
        if not firebase_admin._apps:
            # Initialize Firebase app with the company credentials
            self.app = firebase_admin.initialize_app(self.company_credentials)

        # Set up Firestore with a custom database ID
        dbName = firestoreConfig["dbName"]  # Replace with your Firestore database ID

        self.db = firebase_admin.firestore.client(database_id=dbName)

        # Assign the authentication object
        self.auth = firebase_admin.auth

        # Print a message indicating that Firestore has been initialized
        print(
            f"\033[94mINFO-DB:\033[0m  \033[92mFirestore initialized by\033[0m \033[95m{dbName}\033[0m \033[94mdatabase\033[0m pid: {os.getpid()} {self.db}"
        )

        return (self.db, self.auth)

    def close_firebase(self):
        """Safely closes the Firebase app."""
        if self.app:
            try:
                # Delete the Firebase app
                firebase_admin.delete_app(self.app)
                print("\033[92mINFO-DB:\033[0m Firebase app closed gracefully.")
            except Exception as e:
                print(f"\033[91mERROR-DB:\033[0m Error closing Firebase app: {e}")

    def close(self):
        """Implements the close() method for contextlib.closing compatibility."""
        self.close_firebase()

client: FirebaseClient = FirebaseClient()
# usage
db, auth = client.init_firebase()

def signal_handler(sig, frame):
    """Handles signals for graceful shutdown."""
    print(f"\n\033[93mINFO:\033[0m Firebase Client Received signal {sig}. Shutting down gracefully...")
    import contextlib

    with contextlib.closing(db):  # closes on exit
        client.close()
    sys.exit(0)


# Register signal handlers for SIGINT (Ctrl+C) and SIGTERM (termination signal)
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)
